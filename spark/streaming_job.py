from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define schema
schema = StructType() \
    .add("order_id", StringType()) \
    .add("customer_name", StringType()) \
    .add("amount", DoubleType()) \
    .add("city", StringType()) \
    .add("timestamp", DoubleType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

# Convert value from binary → string → JSON
json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Write to console (for now)
query = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
