import argparse
import atexit
import logging
import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

logger_format = '%(asctime)s - %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)


def shutdown_hook(producer):
    """
	a shutdown hook to be called before the shutdown
	"""
    try:
        logger.info(
            'Flushing pending messages to kafka, timeout is set to 10s')
        producer.flush(10)
        logger.info('Finish flushing pending messages to kafka')
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages to kafka, caused by: %s',
                    kafka_error.message)
    finally:
        try:
            logger.info('Closing kafka connection')
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to close kafka connection, caused by: %s',
                        e.message)


def process_stream(stream, sc, sqlc, kafka_producer, target_topic, batch_duration):
    def send_to_kafka(rdd):
        results = rdd.collect()
        for r in results:
            data = json.dumps({
                'symbol': r[0],
                'ts': time.time(),
                'price': r[1],
                'volume': r[2]
            })
            try:
                logger.info('Sending average price %s to kafka', data)
                kafka_producer.send(target_topic, value=data.encode('utf-8'))
            except KafkaError as error:
                logger.warn('Failed to send average price to kafka: ',
                            error.message)

    def sink_to_hbase(rdd):
        results = rdd.collect()
        row_ts = int(time.time()) // 3600 * 3600
        now_ts = int(time.time()) // batch_duration * batch_duration
        batch_ts_offset = now_ts - row_ts
        for r in results:
            data_price = json.dumps({
                'row': f'{r[0]}.{row_ts}.price',
                batch_ts_offset: f'{r[1]:.2f}',
            })
            data_vol = json.dumps({
                'row': f'{r[0]}.{row_ts}.vol',
                batch_ts_offset: f'{r[2]:.2f}'
            })
            df = sqlc.read.json(sc.parallelize([data_price, data_vol]))
            df.show()
            # ''.join(string.split()) in order to write a multi-line JSON string here.
            write_catalog = json.dumps({
                'table': {'namespace': 'default', 'name': 'coins'},
                'rowkey': 'key',
                'columns': {
                    'row': {'cf': 'rowkey', 'col': 'key', 'type': 'string'},
                    str(batch_ts_offset): {'cf': 't', 'col': str(batch_ts_offset), 'type': 'string'}
                }
            })
            df.write.options(catalog=write_catalog, newtable=5).format(
                'org.apache.spark.sql.execution.datasources.hbase').save()

    def pair(data):
        record = json.loads(data)
        return str(record.get('symbol')), (float(record.get('price')), float(record.get('base_vol')), 1)

    rdd = stream.map(pair).reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])).map(
            lambda kv: (kv[0], kv[1][0] / kv[1][2], kv[1][1] * kv[1][0] / kv[1][2]))
    rdd.foreachRDD(sink_to_hbase)


if __name__ == '__main__':
    # Setup command line arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument('source_topic')
    parser.add_argument('target_topic')
    parser.add_argument('kafka_broker')
    parser.add_argument(
        'batch_duration',
        help='the batch duration in secs')  # spark mini-batch streaming

    # Parse arguments.
    args = parser.parse_args()
    source_topic = args.source_topic
    target_topic = args.target_topic
    kafka_broker = args.kafka_broker
    batch_duration = int(args.batch_duration)

    # Create SparkContext and SteamingContext.
    # https://spark.apache.org/docs/2.2.0/api/python/index.html
    sc = SparkContext('local[2]', 'AveragePrice')  # 2 threads
    sc.setLogLevel('INFO')
    sqlc = SQLContext(sc)
    ssc = StreamingContext(sc, batch_duration)

    # Instantiate a kafka stream for processing.
    directKafkaStream = KafkaUtils.createDirectStream(
        ssc, [source_topic], {'metadata.broker.list': kafka_broker})

    # Extract value from directKafkaStream (key, value) pair.
    stream = directKafkaStream.map(lambda msg: msg[1])

    # Instantiate a simple kafka producer
    kafka_producer = KafkaProducer(bootstrap_servers=kafka_broker)

    process_stream(stream, sc, sqlc, kafka_producer, target_topic, batch_duration)

    # Setup shutdown hook.
    atexit.register(shutdown_hook, kafka_producer)

    ssc.start()
    ssc.awaitTermination()
