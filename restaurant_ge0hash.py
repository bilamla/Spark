from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import pygeohash as pgh

# initialize sparksession
spark = SparkSession.builder.appName("Generate Geohash").getOrCreate()

# function to generate geohash
def generate_geohash(lat, lng):
    if lat is not None and lng is not None:
        return pgh.encode(lat, lng, precision=4)
    return None

# register udf
geohash_udf = udf(generate_geohash, StringType())

# load data
file_path = "/Users/malikaschaker/Desktop/EPAM/Practice_Spark/restaurant_csv/updated_combined_restaurant_data/part-00000-b01c3e06-773c-4602-98ef-b28fe99081ef-c000.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# add geohash column
df_with_geohash = df.withColumn("geohash", geohash_udf(df["lat"], df["lng"]))

# save updated data
output_path = "/Users/malikaschaker/Desktop/EPAM/Practice_Spark/restaurant_parquet/updated_combined_restaurant_data_with_geohash"
df_with_geohash.write.parquet(output_path, mode="overwrite")

# stop spark session
spark.stop()
