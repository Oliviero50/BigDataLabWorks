from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql.functions import current_timestamp

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkHighwayAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema of json
schema = StructType() \
    .add("type", StringType()) \
    .add("highway", StringType()) \
    .add("timestamp", TimestampType())

# Create DataFrame representing the stream of input lines from connection to localhost:9999
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "cartopic") \
    .load()

tmp = df.selectExpr("CAST(value AS STRING)")
cardata = tmp.select(
    from_json(col("value"), schema).alias("data")).select("data.*")

windowedCounts = cardata.groupBy(
    window(cardata.timestamp, "10 seconds", "10 seconds"),
    cardata.type
).count()

query = windowedCounts \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .outputMode("update") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("checkpointLocation", "./checkpoint") \
    .option("topic", "pyspark") \
    .start()

query.awaitTermination()
