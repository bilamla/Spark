from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
import requests
import os
import time

# initialize sparksession
spark = SparkSession.builder.appName("Combine and Update Restaurant Data").getOrCreate()

# opencage api key
API_KEY = "4928fffdeda84430bbd75484f588f927"

# function to get coordinates with retry logic and rate limiting
def get_coordinates(city, country):
    if not city or not country:
        return None, None
    try:
        url = f"https://api.opencagedata.com/geocode/v1/json?q={city},{country}&key={API_KEY}"
        print(f"Requesting: {url}")
        for _ in range(3):  # Retry up to 3 times
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if data.get("results"):
                    location = data["results"][0]["geometry"]
                    return location["lat"], location["lng"]
            elif response.status_code == 429:  # Rate limit exceeded
                print("Rate limit exceeded. Sleeping for 1 second...")
                time.sleep(1)  # Wait before retrying
            else:
                print(f"API error for {city}, {country}: {response.status_code}")
        print(f"Failed to fetch coordinates for {city}, {country} after retries.")
    except Exception as e:
        print(f"Error fetching coordinates for {city}, {country}: {e}")
    return None, None

# udf for geolocation
@udf("struct<lat:double,lng:double>")
def fetch_geolocation(city, country):
    lat, lng = get_coordinates(city, country)
    if lat is not None and lng is not None:
        return {"lat": lat, "lng": lng}
    return None

# combine all csv files from the specified folder
folder_path = "/Users/malikaschaker/Desktop/EPAM/Practice_Spark/restaurant_csv/"
all_csv_files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith(".csv")]

# read and combine all csv files
df = spark.read.csv(all_csv_files, header=True, inferSchema=True)

# filter rows with null coordinates
null_coordinates_df = df.filter((col("lat").isNull()) | (col("lng").isNull()))
print("Rows with NULL latitude or longitude:")
null_coordinates_df.show(truncate=False)

# update lat/lng for rows with null coordinates
updated_null_coordinates_df = null_coordinates_df.withColumn(
    "geolocation", fetch_geolocation(col("city"), col("country"))
).withColumn(
    "lat", when(col("geolocation").isNotNull(), col("geolocation.lat")).otherwise(col("lat"))
).withColumn(
    "lng", when(col("geolocation").isNotNull(), col("geolocation.lng")).otherwise(col("lng"))
).drop("geolocation")

# show updated rows
print("Updated Data for rows with previously NULL latitude or longitude:")
updated_null_coordinates_df.show(truncate=False)

# combine updated rows with the rest of the data
non_null_coordinates_df = df.filter((col("lat").isNotNull()) & (col("lng").isNotNull()))
final_df = non_null_coordinates_df.union(updated_null_coordinates_df)

# save the final DataFrame
output_path = "/Users/malikaschaker/Desktop/EPAM/Practice_Spark/restaurant_csv/updated_combined_restaurant_data"
final_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

print(f"Updated data saved to {output_path}")

# stop spark session
spark.stop()


