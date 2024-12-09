from pyspark.sql import SparkSession

# initialize sparksession
spark = SparkSession.builder.appName("Left Join Weather and Restaurant Data").getOrCreate()

# paths to data
weather_data_path = "/Users/malikaschaker/Desktop/EPAM/Practice_Spark/weather_dataset/combined_weather_data_with_geohash"
restaurant_data_path = "/Users/malikaschaker/Desktop/EPAM/Practice_Spark/restaurant_parquet/updated_combined_restaurant_data_with_geohash"

# load data
weather_df = spark.read.parquet(weather_data_path)
restaurant_df = spark.read.parquet(restaurant_data_path)

# perform left join
joined_df = restaurant_df.join(weather_df, on="geohash", how="left")

joined_df.show(n=200, truncate=False)

# save the joined data
output_path = "/Users/malikaschaker/Desktop/EPAM/Practice_Spark/enriched_weather_restaurant_data"
joined_df.write.partitionBy("geohash").mode("overwrite").parquet(output_path)

spark.stop()

