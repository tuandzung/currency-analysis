import time
import argparse
import dateutil.parser

from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.column import Column, _to_java_column
import pyspark.sql.types as t
import pyspark.sql.functions as F


spark = SparkSession \
        .builder \
        .appName('structuredStreamingKafka') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,za.co.absa:abris_2.11:3.2.1,org.apache.avro:avro:1.9.1') \
        .config('schemaRegistryUrl', 'http://schema-registry:8081') \
        .config('schemaRegistryReadTopic', 'coins_realtime_update') \
        .config('schemaRegistryWriteTopic', 'coins_hbase_sink') \
        .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
sqlc = SQLContext(sc)


def from_avro(col):
    """
    avro deserialize

    :param col: column name "key" or "value"
    :param topic: kafka topic
    :param schema_registry_url: schema registry http address
    :return:
    """
    jvm_gateway = sc._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro
    naming_strategy = getattr(
        getattr(abris_avro.read.confluent.SchemaManager, "SchemaStorageNamingStrategies$"),
        "MODULE$"
    ).TOPIC_NAME()

    schema_registry_config_dict = {
        "schema.registry.url": spark.conf.get("schemaRegistryUrl"),
        "schema.registry.topic": spark.conf.get("schemaRegistryReadTopic"),
        "{col}.schema.id".format(col=col): "latest",
        "{col}.schema.naming.strategy".format(col=col): naming_strategy
    }

    conf_map = getattr(getattr(jvm_gateway.scala.collection.immutable.Map, "EmptyMap$"), "MODULE$")
    for k, v in schema_registry_config_dict.items():
        conf_map = getattr(conf_map, "$plus")(jvm_gateway.scala.Tuple2(k, v))

    return Column(abris_avro.functions.from_confluent_avro(_to_java_column(col), conf_map))


def to_avro(col):
    """
    avro  serialize
    :param col: column name "key" or "value"
    :param topic: kafka topic
    :param schema_registry_url: schema registry http address
    :return:
    """
    jvm_gateway = sc._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro
    naming_strategy = getattr(
        getattr(abris_avro.read.confluent.SchemaManager, "SchemaStorageNamingStrategies$"),
        "MODULE$"
    ).TOPIC_NAME()

    schema_registry_config_dict = {
        "schema.registry.url": spark.conf.get("schemaRegistryUrl"),
        "schema.registry.topic": spark.conf.get("schemaRegistryWriteTopic"),
        "{col}.schema.id".format(col=col): "latest", 
        "{col}.schema.naming.strategy".format(col=col): naming_strategy
    }

    conf_map = getattr(getattr(jvm_gateway.scala.collection.immutable.Map, "EmptyMap$"), "MODULE$")
    for k, v in schema_registry_config_dict.items():
        conf_map = getattr(conf_map, "$plus")(jvm_gateway.scala.Tuple2(k, v))

    return Column(abris_avro.functions.to_confluent_avro(_to_java_column(col), conf_map))


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

    df = spark \
      .readStream \
      .format('kafka') \
      .option('kafka.bootstrap.servers', kafka_broker) \
      .option('startingOffsets', 'latest') \
      .option('subscribe', topic_name) \
      .load() \
      .select(from_avro('value').alias('parsed')) \
      .select('parsed.*')
    
#     query = df.writeStream.format('console').option('truncate', 'false').start().awaitTermination()
    
#     json_schema = '{"schema": {"type": "struct", "name": "coins.ohlc", "fields": [{"field": "exchange", "type": "string", "optional": false}, {"field": "symbol", "type": "string", "optional": false}, {"field": "time", "type": "string", "optional": false}, {"field": "open", "type": "float", "optional": false}, {"field": "high", "type": "float", "optional": false}, {"field": "low", "type": "float", "optional": false}, {"field": "close", "type": "float", "optional": false}]}, '
    
    query = df \
      .withColumn('timestamp', F.col('time').cast('timestamp')) \
      .withWatermark('timestamp', watermark) \
      .groupBy(F.window('timestamp', window), 'exchange', 'symbol') \
      .agg(F.first('price').alias('open'),
           F.max('price').alias('high'),
           F.min('price').alias('low'),
           F.last('price').alias('close'),
           F.last('base_vol_24h').alias('base_vol_24h')) \
      .withColumn('time', F.unix_timestamp('window.end').cast('string')) \
      .withColumn('volume', F.col('base_vol_24h') * F.col('close')) \
      .withColumn('row_id', F.concat(
        'symbol', F.lit('#'), 'exchange', F.lit('#'), 'time')) \
      .withColumn('value', F.struct(
        'row_id', 'open', 'high', 'low', 'close', 'volume')) \
      .select(to_avro('value').alias('value')) \
      .writeStream \
      .format('kafka') \
      .option('kafka.bootstrap.servers', kafka_broker) \
      .option('topic', 'coins_hbase_sink') \
      .option('checkpointLocation', "/tmp/checkpoint") \
      .start() \
      .awaitTermination()
