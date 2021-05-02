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

df.printSchema()
tmp = df.selectExpr("CAST(value AS STRING)")
cardata = tmp.select(
    from_json(col("value"), schema).alias("data")).select("data.*")
cardata.printSchema()

windowedCounts = cardata \
    .groupBy(
        window(cardata.timestamp, "10 seconds", "10 seconds"),
        cardata.type
    ).count()
windowedCounts.printSchema()

# query = windowedCounts.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option('truncate', 'false') \
#     .start()

query = windowedCounts \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .outputMode("complete") \
    .option("checkpointLocation", "./checkpoint") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "pyspark") \
    .start()

query.awaitTermination()
