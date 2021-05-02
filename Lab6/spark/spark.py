from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql.functions import current_timestamp

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkHighwayAnalysis") \
    .getOrCreate()

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
#    .select(from_json(col("value").cast("string"), schema).alias("parsed_value"))

df.printSchema()
tmp = df.selectExpr("CAST(value AS STRING)")
cardata = tmp.select(from_json(col("value"), schema).alias("data")).select("data.*")
# cardata.withColumn("timestamp", col("timstamp").cast(TimestampType))
# dtest = cardata.select()

#dfcar = cardata.select(col("type").alias("type"), col("highway").alias("highway"), from_unixtime(col("timestamp"), "MM-dd-yyyy HH:mm:ss").alias("timestamp"))

# Print output
#query = cardata.writeStream \
#    .outputMode("append") \
#    .format("console") \
#    .start()

windowedCounts = cardata.groupBy(
    window(cardata.timestamp, "10 seconds", "5 seconds"),
    cardata.type
).count()

query = windowedCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option('truncate', 'false') \
    .start()

# Split the lines into words and add timestamp column
#words = df.select(
#   explode(
#       split(df.value, " ")
#   ).alias("word")
#).withColumn("timestamp", current_timestamp())

# Generate running word count
#wordCounts = words.groupBy("word").count()

#query = wordCounts \
#    .writeStream \
#    .outputMode("complete") \
#    .format("console") \
#    .start()

# Send words back to Kafka
# query = words \
#     .selectExpr("CAST(timestamp AS STRING) AS key", "CAST(word AS STRING) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .option("checkpointLocation", "./checkpoint") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("topic", "pyspark") \
#     .start() 

# Send words back to Kafka
#query = tresult \
#    .writeStream \
#    .format("kafka") \
#    .option("checkpointLocation", "./checkpoint") \
#    .option("kafka.bootstrap.servers", "localhost:9092") \
#    .option("topic", "pyspark") \
#    .start() 


query.awaitTermination()