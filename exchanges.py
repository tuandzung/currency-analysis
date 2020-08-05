import argparse
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.column import Column, _to_java_column
import pyspark.sql.types as t
import pyspark.sql.functions as F


parser = argparse.ArgumentParser()
parser.add_argument('-r', '--read-topic', help='the kafka topic to read data from.')
parser.add_argument('-s', '--sink-topic', help='the kafka topic to sink data to.')
parser.add_argument('-b', '--broker', default='kafka:29092', help='the kafka bootstrap server.')
parser.add_argument('-u', '--schema-registry-url', default='http://schema-registry:8081', help='the schema registry server.')
parser.add_argument('-w', '--window', help='aggregrate window.')    

# Parse arguments.
args = parser.parse_args()
schema_registry_url = args.schema_registry_url
read_topic = args.read_topic
sink_topic = args.sink_topic
kafka_broker = args.broker
window = args.window


spark = SparkSession \
        .builder \
        .appName('structuredStreamingKafka') \
        .config('schemaRegistryUrl', schema_registry_url) \
        .config('kafkaBroker', kafka_broker) \
        .config('readTopic', read_topic) \
        .config('writeTopic', sink_topic) \
        .config('window', window) \
        .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
sqlc = SQLContext(sc)


def from_avro(col):
    jvm_gateway = sc._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro
    naming_strategy = getattr(
        getattr(abris_avro.read.confluent.SchemaManager, "SchemaStorageNamingStrategies$"),
        "MODULE$"
    ).TOPIC_NAME()

    schema_registry_config_dict = {
        "schema.registry.url": spark.conf.get("schemaRegistryUrl"),
        "schema.registry.topic": spark.conf.get("readTopic"),
        "{col}.schema.id".format(col=col): "latest",
        "{col}.schema.naming.strategy".format(col=col): naming_strategy
    }

    conf_map = getattr(getattr(jvm_gateway.scala.collection.immutable.Map, "EmptyMap$"), "MODULE$")
    for k, v in schema_registry_config_dict.items():
        conf_map = getattr(conf_map, "$plus")(jvm_gateway.scala.Tuple2(k, v))

    return Column(abris_avro.functions.from_confluent_avro(_to_java_column(col), conf_map))


def to_avro(col):
    jvm_gateway = sc._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro
    naming_strategy = getattr(
        getattr(abris_avro.read.confluent.SchemaManager, "SchemaStorageNamingStrategies$"),
        "MODULE$"
    ).TOPIC_NAME()

    schema_registry_config_dict = {
        "schema.registry.url": spark.conf.get("schemaRegistryUrl"),
        "schema.registry.topic": spark.conf.get("writeTopic"),
        "{col}.schema.id".format(col=col): "latest", 
        "{col}.schema.naming.strategy".format(col=col): naming_strategy
    }

    conf_map = getattr(getattr(jvm_gateway.scala.collection.immutable.Map, "EmptyMap$"), "MODULE$")
    for k, v in schema_registry_config_dict.items():
        conf_map = getattr(conf_map, "$plus")(jvm_gateway.scala.Tuple2(k, v))

    return Column(abris_avro.functions.to_confluent_avro(_to_java_column(col), conf_map))


if __name__ == '__main__':
    df = spark \
      .readStream \
      .format('kafka') \
      .option('kafka.bootstrap.servers', spark.conf.get('kafkaBroker')) \
      .option('startingOffsets', 'latest') \
      .option('subscribe', spark.conf.get('readTopic')) \
      .load() \
      .select(from_avro('value').alias('parsed')) \
      .select('parsed.*')

    query = df \
      .withColumn('timestamp', F.col('time').cast('timestamp')) \
      .withWatermark('timestamp', '1 minute') \
      .groupBy(F.window('timestamp', window), 'exchange', 'symbol') \
      .agg(F.first('price').alias('open'),
           F.max('price').alias('high'),
           F.min('price').alias('low'),
           F.last('price').alias('close'),
           F.sum('volume').alias('volume')) \
      .withColumn('time', F.unix_timestamp('window.end').cast('string')) \
      .withColumn('row_id', F.concat(
        'symbol', F.lit('#'), 'exchange', F.lit('#'), 'time')) \
      .withColumn('value', F.struct(
        'row_id', 'open', 'high', 'low', 'close', 'volume')) \
      .select(to_avro('value').alias('value')) \
      .writeStream \
      .format('kafka') \
      .option('kafka.bootstrap.servers', spark.conf.get('kafkaBroker')) \
      .option('topic', spark.conf.get("writeTopic")) \
      .option('checkpointLocation', "hdfs://hdfs-namenode:8020/tmp/checkpoint") \
      .start() \
      .awaitTermination()
