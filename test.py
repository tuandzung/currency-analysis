from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql.functions import col, from_json
import time

spark = SparkSession \
        .builder \
        .appName("test") \
        .getOrCreate() 

sc = spark.sparkContext
sqlc = SQLContext(sc)
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:29092") \
  .option("startingOffsets", "earliest") \
  .option("subscribe", "analyzer") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# # value schema: { "a": 1, "b": "string" }
# schema = StructType([
#     StructField("symbol", StringType(), True),
#     StructField("price", StringType(), True),
#     StructField("ts", StringType(), True)
# ])
# df.select( \
#   col("key").cast("string"),
#   from_json(col("value").cast("string"), schema))

# Start running the query that prints the running counts to the console
query = df \
    .writeStream \
    .format('console') \
    .start()

query.awaitTermination()