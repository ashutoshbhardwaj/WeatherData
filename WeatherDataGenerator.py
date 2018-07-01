
# # Program: Weather Test Data Generator
# # Date : 1st July, 2018
# # Author : Ashutosh Bhardwaj
# 
# # Narrative: 
# 
# 1. This application takes any number of coordinates(Latitude, Longitude) within Australia as 
#    an input file with format -
#                         place,state,latitude,longitude
#                         ------------------ ----------
#                         Abbeywood,QLD,-26.10688,151.6288
# 
#     For sake of convienence, provided a list of towns(some 400+) with their latitudes and 
#     longitudes, downloaded from https://www.australiantownslist.com/. One can create your 
#     own list of arbitrary number of coordinates.
# 
# 2. Weather data for 49 Australian stations dataset from Kaggle - https://www.kaggle.com/jsphyg/weather-dataset-rattle-package
#    The coordinates(latitude,langitude) of weather stations are collected using Google API. Seperate program for that. 
# 
# 3. Then it  calculate the distance of given list of coordinates from the coordinates of Weather 
#    Stations(49 precisely) fetched from reference file (yyyy) using Haversine formula. 
#    The haversine formula determines the great-circle distance between two points on a sphere given their longitudes and latitudes.
#    Details - https://en.wikipedia.org/wiki/Haversine_formula
# 
# 4. Sort the list with computed distance of given coordinates from different weather staions
#    and pick the closest weather station to the given coordinate.Now you have list of provided coordinates with their 
#    closest weather station. 
# 
# 5. In the dataframe generated in step 3, add another column with random date.
# 
# 6. For the random date, read the Historical Weather data emitted for particular station on that random date.
# 
# 7. Based on the input provided by the weather station, generate the sample weather data for given coordinates, 
#    join with the IATA/IACO codes of Airport stations and write it to sample output file.
# 
# 
# # Files - 
# Inbound Files -  
#     Input File - 
#         1. List of coordinates(latitudes and longitudes) of 400+ Australian towns. - 
#         path_to_au_towns_file="Data/input/au-towns-sample.csv"
#     Reference Files - 
#         2. Weather Data from Weather Monitoring Stations - 
#         path_to_weather_data_file= "Data/input/weatherAUS.csv"
#         3. IATA/IACO Codes for Airport near to Weather Stations - 
#         path_to_iata_codes_file= "Data/input/iatacodes.csv"
#         4. Weather Station with their Latitudes and Longitudes - 
#         path_to_weather_station_lat_lang_file= "Data/input/station_lat_lang.csv"
#         
# Outbound Files -
#         1. File in required format having sample weather data 
#         output_path_to_generated_weather_data_file = "Data/output/outputSampleWeather"



# # Boiler plate code for spark 

from pyspark.sql.functions import *
import random,sys
from pyspark.sql.functions import rand,when
from pyspark.sql.functions import date_add, date_sub
from pyspark.sql.types import DateType
from datetime import datetime,timedelta,date
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

# Working on local mode so there is no point of having 200 shuffle partition by default
spark.conf.set("spark.sql.shuffle.partitions", 8)

########################### INPUT PARAMETERS #########################################
## Input and Reference files -
path_to_au_towns_file="Data/input/au-towns-sample.csv"
path_to_weather_data_file= "Data/input/weatherAUS.csv"
path_to_iata_codes_file="Data/input/iatacodes.csv"
path_to_weather_station_lat_lang_file="Data/input/station_lat_lang.csv"
## Output File -
output_path_to_generated_weather_data_file = "Data/output/outputSampleWeather"
#output_path_to_generated_weather_data_file = "/home/ashutosh/outputSampleWeather"

# Number of towns you want in output file. Don't exceed more than 450 as we have only 450 coordinates 
numberOfSampleCities=400
##
#######################################################################################

if numberOfSampleCities > 440:
    print(">>>>>>>>>>> ERROR >>>>>: Please limit the number below 440 as we have just 440+ coordinates in au-towns-sample.csv")
    sys.exit(1)

# Note : Intentionally reading data using inferSchema = true as the data contains data as string in many rows
# Also just to demonstrate that in most of the cases, data munging requires on the fly data casting to handle 
#
# # Create a dataframe of Weather Data from 49 Weather Stations  

df_WSD=spark.read.load(path_to_weather_data_file,
                    format="csv", 
                    sep=",", 
                    inferSchema="true", 
                    header="true")
df_WSD.cache()

# # Weather Station with their Latitudes and Longitudes

df_WS_lat_lang=spark.read.load(path_to_weather_station_lat_lang_file,
                     format="csv", 
                    sep=",", 
                    inferSchema="true", 
                    header="true")
df_WS_lat_lang.cache()

## Create coordinate in memory table pointing to df_WS_lat_lang dataframe to run sql against it
df_WS_lat_lang.createOrReplaceTempView("coordinates")

# # List of IATA/IACO codes corresponding to all 49 weather stations , will use it while writing output file

df_station_names_iata=spark.read.load(path_to_iata_codes_file,
                     format="csv", sep=",", inferSchema="true", header="True")
df_station_names_iata.cache()

# # List of 450+ Australian towns with Latitudes and Longitudes for Generating sample data file 

df_au_towns=spark.read.load(path_to_au_towns_file,
                     format="csv", sep=",", inferSchema="true", header="true")

# # Randomly select given number of sample Cities from the list of 450+ Australian towns

listOfCities=df_au_towns.rdd.map(lambda x:(x[0]+x[1],x[2],x[3]))\
                            .takeSample(False, numberOfSampleCities, seed=random.randrange(1,100,1))

#  Create Dataframe of randomly selected  sample list of cities/towns

df_random_towns=spark.createDataFrame(listOfCities,["city","latitude","longitude"])


df_random_towns.createOrReplaceTempView("targetstations")


## Calculate the distance of each randomly chosen town from each weather station by using harvsine formula.
## The haversine formula determines the great-circle distance between two points on a sphere given their longitudes and latitudes.
## Example SQL can be found on stackexchange - 
## https://stackoverflow.com/questions/11112926/how-to-find-nearest-location-using-latitude-and-longitude-from-sql-database
## Tested the distance return by query for couple of coordinates with Google :), got close figures.

df_distance=spark.sql("""SELECT coordinates.city,
                        coordinates.latitude,
                        coordinates.longitude,
                        targetstations.city as target_city,
                        targetstations.latitude as target_latitude,
                        targetstations.longitude as target_longitude,
                        int(( 3959 * acos( cos( radians(targetstations.latitude) ) * 
                            cos( radians( coordinates.latitude ) ) * 
                            cos( radians( coordinates.longitude) - radians(targetstations.longitude) ) 
                            + sin( radians(targetstations.latitude) ) *  
                            sin( radians( coordinates.latitude ) ) ) )) AS distance 
                FROM coordinates cross join  targetstations """)
df_distance.cache()
## In memory table to run sql against it

df_distance.createOrReplaceTempView("Output")


## Mark the row number based on distance of each town from all weather station.
## Then pick the one with the least distance.

df_closest_station_to_town = spark.sql("""select city,
                                    latitude,
                                    longitude,
                                    target_city,
                                    round(target_latitude,2) as target_latitude,
                                    round(target_longitude,2) as target_longitude,
                                    distance, 
                                    row_number() over (partition by target_city order by distance)
                                                    as least_distance
                                    from Output having least_distance = 1""")


# User Define Function to generate random date of 2016

random_date=udf(lambda s: date(2016, 1, 1) + timedelta(days=int((s*1000)%365)), DateType())


# Add a column to the dataframe(closest station to town) with Random number and 
# seed that random number to generate random date of 2016 using UDF defined at above step 

list_of_cities=df_closest_station_to_town.withColumn('Random', rand())\
                                         .withColumn("Random_Date",random_date('Random'))

list_of_cities.cache()


# Join the dataframes - list of cities with weather dataframe to get the weather information for random  selected Australian towns

JoinedDataFrame=df_WSD.join(list_of_cities,(df_WSD.Location==list_of_cities.city) & 
                     (df_WSD.Date.cast("Date") == list_of_cities.Random_Date))


# Based on weather data fetched from weather station for that location and that date, generate the data accordingly.
# Could have cleaned data but to show that usual data comes with garbage/empty/null data,handling data with NA on the fly
                                                                 
outputFrame=JoinedDataFrame.withColumn("Condition",when(JoinedDataFrame.RainToday == 'Yes', "Rainy")
                    # Generate the Condition column data for rainy situtations 
                    .when((JoinedDataFrame.Rainfall!='NA')&(JoinedDataFrame.Rainfall.cast("Float")>20), "Rainy")
                    .when((JoinedDataFrame.WindGustSpeed != 'NA')&(JoinedDataFrame.WindGustSpeed.cast("Integer") > 50), "Windy")
                    .when((JoinedDataFrame.Pressure9am != 'NA')&(JoinedDataFrame.Pressure9am.cast("Float") < 100 ),"Windy")
                    # Handling Humid Weather      
                    .when((JoinedDataFrame.Humidity9am != 'NA')&(JoinedDataFrame.Humidity9am.cast("Float") > 85), "Humid")
                    # Handling weather with Sunshine intensity
                    .when(((JoinedDataFrame.Sunshine!='NA')&(JoinedDataFrame.Sunshine.cast("Float") <5)),"Cloudy")
                    .when(((JoinedDataFrame.Sunshine!='NA')&(JoinedDataFrame.Sunshine.cast("Float") > 5)&
                        (JoinedDataFrame.Sunshine.cast("Float") <10)),"Partial Cloudy")
                    .when(((JoinedDataFrame.Sunshine!='NA')&(JoinedDataFrame.Sunshine.cast("Float") > 10)),"Sunny")
                    # Defaulting col with NA
                    .otherwise(lit("NA")))

## Data fixing for the rows having no data 

outputFrame_filled_NA = outputFrame.withColumn("AvgTemp",when((outputFrame.MinTemp == 'NA')|(outputFrame.MaxTemp == 'NA'),"20")
                        .otherwise(round((outputFrame.MinTemp.cast("Float") + outputFrame.MaxTemp.cast("Float"))/2,1) ))\
                        .withColumn("RefinedCond",when(outputFrame.Condition == "NA","Clear Sky").otherwise(outputFrame.Condition))\
                        .withColumn("Pressure",when(outputFrame.Pressure9am == 'NA',"1001").otherwise(outputFrame.Pressure9am))\
                        .withColumn("Humidity",when(outputFrame.Humidity9am == 'NA', "55").otherwise(outputFrame.Humidity9am))
                                      
                                             
# Broadcast the IATA codes file and join it with Outputframe having all the weather information.
# Select required fields from the joined table. 

csvFile= outputFrame_filled_NA.join(broadcast(df_station_names_iata),
                          outputFrame_filled_NA.Location == df_station_names_iata.Location_Name)\
                    .select("IATA_ICAO_CODE",
                            concat_ws(',',"target_latitude","target_longitude"),
                            "Date",
                            "RefinedCond",
                            "AvgTemp",
                            "Pressure",
                            "Humidity").coalesce(1)

### Write the output file in required format
### Field 1 - IATA/ICAO code for nearest station
### Field 2 - Latitude,Longitude of the randomly selected town from 450 AU towns list.
### Field 3 - Timestamp of recorded weather data, date selected randomly in year 2016
### Field 4-  Condition like - Rainy,Humid,Cloudy,Partial Cloudy,Clear Sky, Sunny based on clues from data
### Field 5 - Average temperature of the day 
### Field 6 - Atmospheric Pressure
### Field 7 - Humidity 

csvFile.write.format("csv").mode("overwrite").option("sep","|").save(output_path_to_generated_weather_data_file)

