from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

from pyspark.sql.functions import current_timestamp


spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "testtopic") \
    .load()

df.printSchema()
df.selectExpr("CAST(value AS STRING)")

# Split the lines into words and add timestamp column
words = df.select(
   explode(
       split(df.value, " ")
   ).alias("word")
).withColumn("timestamp", current_timestamp())

# Generate running word count
wordCounts = words.groupBy("word").count()

query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Send words back to Kafka
# query = words \
#     .selectExpr("CAST(timestamp AS STRING) AS key", "CAST(word AS STRING) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .option("checkpointLocation", "./checkpoint") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("topic", "pyspark") \
#     .start() 

query.awaitTermination()