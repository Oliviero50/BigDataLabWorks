from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import length

spark = SparkSession \
    .builder \
    .appName("kappa") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "testtopic") \
    .load()

# Count msg length
words = df.withColumn("timestamp", current_timestamp()) \
    .withColumn("word", length(df.value))

# Send back to Kafka
query = words \
    .selectExpr("CAST(timestamp AS STRING) AS key", "CAST(word AS STRING) AS value") \
    .writeStream \
    .format("kafka") \
    .option("checkpointLocation", "./checkpoint") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "pyspark") \
    .start()

query.awaitTermination()