# Spark

Weather and Restaurant Data Processing

Project Overview

This project is about processing weather and restaurant data:

It combines weather files stored in .parquet format from multiple folders into one dataset.

It creates a geohash column using latitude (lat) and longitude (lng).

It joins weather and restaurant data based on the geohash.

It fixes missing restaurant coordinates using the OpenCage Geocoding API.

It saves the final result as a .parquet file.


Features

1. Combine Weather Data:
Reads all .parquet files from the weather_dataset folder.
Combines them into one dataset.
Adds a geohash column for easier matching.
2. Fix Restaurant Data:
Finds rows with missing latitude (lat) or longitude (lng).
Updates these values using the OpenCage API.
3. Join Datasets:
Combines weather and restaurant data into one table.
4. Save Final Data:
Saves the output as a .parquet file for easy storage and use.

Project Structure

1. scripts
   
1.1. restaurant.py                  # Combines restaurant data in folder and fixes missing coordinated

1.2. restaurant_ge0hash.py          # Adds geohash to restaurant data

1.3.combined_weather_geohash.py    # Combines weather data in folder and adds geohash

1.4.left_join.py                   # Joins weather and restaurant data

2. tests

2.1. test_generate_geohash.py       # Tests for geohash creation

2.2. test_file_processing.py        # Tests for combining files

2.3. test_join_operations.py        # Tests for joining datasets

   
3. data
   
3.1. weather_dataset/               # Original weather data\

3.2. restaurant_csv/                # Original restaurant data

   
4. output
   
4.1. combined_weather_data.parquet  # Combined weather data with geohash

4.2. enriched_data.parquet          # Final output with weather and restaurants

   
5. README.md                          # This file

How to Use

Prerequisites:
Python 3.8 or newer
Apache Spark installed
Libraries: pyspark, pygeohash, requests

Setup:
Download the project:
git clone https://github.com/your-repo/weather_restaurant_project.git
cd weather_restaurant_project

Install the required libraries:
pip install -r requirements.txt

Steps to Run:
1. Combine Weather Data: Run the script to combine weather data and create the geohash column:


python scripts/combine_weather_data.py


2. Fix Missing Restaurant Coordinates: Run the script to update restaurant rows with missing latitude and longitude:

python scripts/update_restaurant_data.py


3. Join Datasets: Run the script to join the weather and restaurant data:


python scripts/join_weather_restaurant.py



4. Check Results: The final data will be saved in the output/ folder as .parquet files.

This project includes unit tests for key functions. To run the tests:
python -m unittest discover tests

Author

Name: Malika Schaker

Email: malikaschaker@gmail.com

GitHub: github.com/bilamla

