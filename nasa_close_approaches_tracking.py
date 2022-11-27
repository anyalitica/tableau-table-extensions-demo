
# This script allows you to connect to NASA's NeoWs (Near Earth Object Web Service) API, 
# and get the dataframe with all NEOs close to earth from today and 7 days ahead

# Import libraries used

import requests
import pandas as pd
from datetime import datetime, date, timedelta

# Create a variable for the API key. You can generate your free key here: https://api.nasa.gov/
api_key = API_KEY

# Set start date to be today
start_date = datetime.today()

# And end date 7 days from today. The longest period that we can query this API is 7 days.
end_date = start_date + timedelta(days=7)

# convert start and end dates to strings for the URL
start_date = start_date.strftime('%Y-%m-%d')
end_date = end_date.strftime('%Y-%m-%d')

# Construct the URL for the API request
url = f'https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key={api_key}'

# Send the URL to the API and get the response code to check the status of the request
response_code = requests.get(url)

# Get the response from the API in JSON format
response = response_code.json()

# The block below iterates through every day and every object data to get the required data points, and updates the dataframe

# The term LD (Lunar Distance) refers to the average distance between the Earth and Moon. 
# For data reported on this site, we use a mean semimajor axis for the moon of 384400 km (~.002570 au) to define one LD.

# create an empty dataframe with the final structure
dataframe = pd.DataFrame(columns = ['id'
                                    , 'name'
                                    , 'nasa_jpl_url'
                                    , 'absolute_magnitude_h'
                                    , 'estimated_diameter_min_meters'
                                    , 'estimated_diameter_max_meters'
                                    , 'is_potentially_hazardous_asteroid'
                                    , 'is_sentry_object'
                                    , 'close_approach_datetime'
                                    , 'relative_velocity_km_sec'
                                    , 'relative_velocity_km_hour'
                                    , 'miss_distance_astronomical'
                                    , 'miss_distance_lunar'
                                    , 'miss_distance_km'
                                    , 'orbiting_body'])

# Iterate through every date of close approach to get data on the asteroids per day

for date in response['near_earth_objects']:
  for i in response['near_earth_objects'][date]:
    id = i['id']
    name = i['name']
    nasa_jpl_url = i['nasa_jpl_url']
    absolute_magnitude_h = i['absolute_magnitude_h']
    estimated_diameter_min_meters = i['estimated_diameter']['meters']['estimated_diameter_min']
    estimated_diameter_max_meters = i['estimated_diameter']['meters']['estimated_diameter_max']
    is_potentially_hazardous_asteroid = i['is_potentially_hazardous_asteroid']
    is_sentry_object = i['is_sentry_object']
    # Convert close approach date time from epoch to UNIX format
    close_approach_epoch = i['close_approach_data'][0]['epoch_date_close_approach']
    close_approach_datetime = datetime.fromtimestamp(close_approach_epoch/1000).strftime('%Y-%m-%d %H:%M:%S')
    relative_velocity_km_sec = i['close_approach_data'][0]['relative_velocity']['kilometers_per_second']
    relative_velocity_km_hour = i['close_approach_data'][0]['relative_velocity']['kilometers_per_hour']
    miss_distance_astronomical = i['close_approach_data'][0]['miss_distance']['astronomical']
    miss_distance_lunar = i['close_approach_data'][0]['miss_distance']['lunar']
    miss_distance_km = i['close_approach_data'][0]['miss_distance']['kilometers']
    orbiting_body = i['close_approach_data'][0]['orbiting_body']

# update the original dataframe with extracted values
    dataframe_date = pd.DataFrame([[ id
                                    , name
                                    , nasa_jpl_url
                                    , absolute_magnitude_h
                                    , estimated_diameter_min_meters
                                    , estimated_diameter_max_meters
                                    , is_potentially_hazardous_asteroid
                                    , is_sentry_object
                                    , close_approach_datetime
                                    , relative_velocity_km_sec
                                    , relative_velocity_km_hour
                                    , miss_distance_astronomical
                                    , miss_distance_lunar
                                    , miss_distance_km
                                    , orbiting_body]],
                                  columns=[
                                    'id'
                                    ,'name'
                                    ,'nasa_jpl_url'
                                    ,'absolute_magnitude_h'
                                    , 'estimated_diameter_min_meters'
                                    , 'estimated_diameter_max_meters'
                                    , 'is_potentially_hazardous_asteroid'
                                    , 'is_sentry_object'
                                    , 'close_approach_datetime'
                                    , 'relative_velocity_km_sec'
                                    , 'relative_velocity_km_hour'
                                    , 'miss_distance_astronomical'
                                    , 'miss_distance_lunar'
                                    , 'miss_distance_km'
                                    , 'orbiting_body'])
  
# append new data points to the original dataframe 

    dataframe = pd.concat([dataframe,dataframe_date])

# reset the dataframe's index
dataframe_final = dataframe.reset_index(drop=True)

# convert the dataframe to list to pass back to Tableau
return dataframe_final.to_dict(orient="list")
