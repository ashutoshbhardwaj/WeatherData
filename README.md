# WeatherData

Objective: To generate "plausible" sample weather data for locations spread across a continent based on data emitted from weather stations.


# Apporach taken: 
Developed a Pyspark application to generate the 'plausible' weather data based on the approach mentioned below - 

1. WeatherDataGenerator program takes any number of coordinates(Latitude, Longitude) within Australia as an input file with format -
                        name,state,latitude,longitude
                        ------------------ ----------
                        Abbeywood,QLD,-26.10688,151.6288

    For sake of convienence, provided a list of towns(some 400+) with their latitudes and longitudes, downloaded from https://www.australiantownslist.com/. One can create your own list of arbitrary number of coordinates.

2. Weather data for 49 Australian stations dataset from Kaggle - https://www.kaggle.com/jsphyg/weather-dataset-rattle-package
The coordinates(latitude,langitude) of weather stations are collected using Google API. Seperate program for that. 

3. Then it  calculate the distance of given list of coordinates from the coordinates of Weather Stations(49 precisely) fetched from reference file (yyyy) using Haversine formula. The haversine formula determines the great-circle distance between two points on a sphere given their longitudes and latitudes. Details - https://en.wikipedia.org/wiki/Haversine_formula

4. Sort the list with computed distance of given coordinates from different weather staions(49 precisely) and pick the closest weather station to the given coordinate.Now you have list of provided coordinates with their closest weather station. 

5. In the dataframe generated in step 3, add another column with random date.

6. For the random date, read the Historical Weather data emitted for particular station on that random date.

7. Based on the input provided by the weather station, generate the sample weather data for given coordinates, join with the IATA/IACO codes of Airport stations and write it to sample output file.


# Files - 
Inbound Files -  
    Input File - 
        1. List of coordinates(latitudes and longitudes) of 400+ Australian towns. - 
        path_to_au_towns_file="Data/input/au-towns-sample.csv"
    Reference Files - 
        2. Weather Data from Weather Monitoring Stations - 
        path_to_weather_data_file= "Data/input/weatherAUS.csv"
        3. IATA/IACO Codes for Airport near to Weather Stations - 
        path_to_iata_codes_file= "Data/input/iatacodes.csv"
        4. Weather Station with their Latitudes and Longitudes - 
        path_to_weather_station_lat_lang_file= "Data/input/station_lat_lang.csv"
        
Outbound Files -
        1. File in required format having sample weather data 
        output_path_to_generated_weather_data_file = "Data/output/outputSampleWeather"
        
        
        
# How to run: 
Copy the folder to your directory and "CD" to that directory.
$ YOUR_SPARK_HOME/bin/spark-submit WeatherDataGenerator.py

