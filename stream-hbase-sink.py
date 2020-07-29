import time
import argparse
import dateutil.parser

from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StringType, StructField, TimestampType
import pyspark.sql.functions as F


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--topic-name', help='the kafka topic push to.')
    # kafka's location, easily handle to remote kafka
    parser.add_argument('-b', '--broker', help='the kafka topic push to.')
    parser.add_argument('-w', '--window', help='aggregrate window.')
    parser.add_argument('-m', '--watermark', help='watermark.')
    parser.add_argument('--table-name', help='hbase table.')
    

    # Parse arguments.
    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.broker
    window = args.window
    watermark = args.watermark
    table_name = args.table_name
    
    spark = SparkSession \
            .builder \
            .appName('structuredStreamingKafka') \
            .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sqlc = SQLContext(sc)

    schema = StructType([
        StructField("exchange", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", StringType(), True),
        StructField("ts", StringType(), True)
    ])
    
    def process_batch(df, epoch_id):
        row_ts = int(time.time()) // 3600 * 3600
        if df.collect():
            df.show(10, False)
            ts = dateutil.parser.parse(df.collect()[0]['window']['end']).timestamp()
            print(ts)
#         write_catalog = json.dumps({
#             'table': {'namespace': 'default', 'name': 'coins'},
#             'rowkey': 'key',
#             'columns': {
#                 'row': {'cf': 'rowkey', 'col': 'key', 'type': 'string'},
#                 str(batch_ts_offset): {'cf': 't', 'col': str(batch_ts_offset), 'type': 'string'}
#             }
#         })

    df = spark \
      .readStream \
      .format('kafka') \
      .option('kafka.bootstrap.servers', kafka_broker) \
      .option('startingOffsets', 'latest') \
      .option('subscribe', topic_name) \
      .load() \
      .select(F.from_json(F.col('value').cast('string'), schema).alias('parsed')) \
      .select('parsed.*')
    
    query = df \
      .withColumn('timestamp', F.from_unixtime(F.col('ts')).cast('timestamp')) \
      .withWatermark('timestamp', watermark) \
      .groupBy(
        F.window('timestamp', window),
        'exchange', 'symbol') \
      .agg(F.first('price').alias('open'),
           F.max('price').alias('high'),
           F.min('price').alias('low'),
           F.last('price').alias('close')) \
      .writeStream \
      .foreachBatch(process_batch) \
      .start() \
      .awaitTermination()
