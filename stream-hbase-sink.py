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

    df = spark \
      .readStream \
      .format('kafka') \
      .option('kafka.bootstrap.servers', kafka_broker) \
      .option('startingOffsets', 'latest') \
      .option('subscribe', topic_name) \
      .load() \
      .select(F.from_json(F.col('value').cast('string'), schema).alias('parsed')) \
      .select('parsed.*')
    
    json_schema = '{"schema": {"type": "struct", "name": "coins.ohlc", "fields": [{"field": "exchange", "type": "string", "optional": false}, {"field": "symbol", "type": "string", "optional": false}, {"field": "time", "type": "string", "optional": false}, {"field": "open", "type": "float", "optional": false}, {"field": "high", "type": "float", "optional": false}, {"field": "low", "type": "float", "optional": false}, {"field": "close", "type": "float", "optional": false}]}, '
    
    query = df \
      .withColumn('timestamp', F.from_unixtime(F.col('ts')).cast('timestamp')) \
      .withWatermark('timestamp', watermark) \
      .groupBy(F.window('timestamp', window),
               'exchange', 'symbol') \
      .agg(F.first('price').cast('float').alias('open'),
           F.max('price').cast('float').alias('high'),
           F.min('price').cast('float').alias('low'),
           F.last('price').cast('float').alias('close')) \
      .withColumn('time', F.unix_timestamp('window.end').cast('string')) \
      .select(F.to_json(F.struct(
        'exchange', 'symbol', 'time',
        'open', 'high', 'low', 'close')).alias('payload')) \
      .select(F.concat(
        F.lit(json_schema),
        F.lit('"payload": '),
        F.col('payload'),
        F.lit('}')).alias('value')) \
      .writeStream \
      .format('kafka') \
      .option('kafka.bootstrap.servers', kafka_broker) \
      .option('topic', 'sink-topic') \
      .option('checkpointLocation', "/tmp/checkpoint") \
      .start() \
      .awaitTermination()
