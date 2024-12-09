from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import pygeohash as pgh
import os

# initialize sparksession
spark = SparkSession.builder \
    .appName("Combine Weather Data and Generate Geohash") \
    .getOrCreate()

# function to generate geohash
def generate_geohash(lat, lng):
    if lat is not None and lng is not None:
        return pgh.encode(lat, lng, precision=4)
    return None

# register udf
geohash_udf = udf(generate_geohash, StringType())

# base path for weather dataset
base_path = "/Users/malikaschaker/Desktop/EPAM/Practice_Spark/weather_dataset/weather-1"

# find all parquet files recursively
def find_parquet_files(base_path):
    all_parquet_files = []
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith(".parquet"):
                all_parquet_files.append(os.path.join(root, file))
    return all_parquet_files

# collect all parquet files
all_parquet_files = find_parquet_files(base_path)
if not all_parquet_files:
    print("No parquet files found. Exiting.")
    exit()

print(f"Found {len(all_parquet_files)} parquet files.")

# read and combine all parquet files
weather_df = spark.read.parquet(*all_parquet_files)
print(f"Loaded {weather_df.count()} records from parquet files.")

# add geohash column
print("Generating geohash column...")
weather_df_with_geohash = weather_df.withColumn("geohash", geohash_udf(col("lat"), col("lng")))

# save combined and updated data
output_path = "/Users/malikaschaker/Desktop/EPAM/Practice_Spark/weather_dataset/combined_weather_data_with_geohash"
print(f"Saving updated data to {output_path}...")
weather_df_with_geohash.coalesce(1).write.mode("overwrite").parquet(output_path)

print(f"Combined and updated weather data saved successfully to {output_path}")

spark.stop()

