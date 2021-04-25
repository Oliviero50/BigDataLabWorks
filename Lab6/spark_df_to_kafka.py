from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import length



spark = SparkSession \
    .builder \
    .appName("kappa") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "testtopic") \
    .load()

df.printSchema()

# Split the lines into words
words = df.select(
   explode(
       split(df.value, " ")
   ).alias("word")
).withColumn("timestamp", current_timestamp())



# Generate running word count
wordCounts = words \
    .withWatermark("timestamp", "1 minutes") \
    .groupBy("word", "timestamp") \
    .count()


ds = wordCounts \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("checkpointLocation", "./checkpoint") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "pyspark") \
    .start()   

ds.awaitTermination()