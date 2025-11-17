from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, max, min
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("WeatherStreamingAnalytics")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
    .config("spark.driver.host", "127.0.0.1")  # Helps avoid loopback
    .getOrCreate()
)


spark.sparkContext.setLogLevel("WARN")

# Define schema for weather data (compatible with WeatherAPI-mapped JSON)
weather_schema = StructType([
    StructField("city", StringType()),
    StructField("temperature", DoubleType()),
    StructField("feels_like", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("weather", StringType()),
    StructField("description", StringType()),
    StructField("wind_speed", DoubleType()),  
    StructField("timestamp", StringType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-data") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON data
weather_df = df.select(
    from_json(col("value").cast("string"), weather_schema).alias("data")
).select("data.*")

# Perform analytics
city_stats = weather_df.groupBy("city") \
    .agg(
        avg("temperature").alias("avg_temp"),
        max("temperature").alias("max_temp"),
        min("temperature").alias("min_temp"),
        avg("humidity").alias("avg_humidity")
    )

# Display streaming data
query1 = weather_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query2 = city_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

print("\n Spark Streaming Started!")
print("Monitoring weather data stream...\n")

spark.streams.awaitAnyTermination()