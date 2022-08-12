# -*- coding: utf-8 -*-
"""
Created on Sat Nov 13 22:51:52 2021

@author: Dylan
"""
import pandas as pd
from pandasql import sqldf
import os
import glob
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

path = 'C:\\Users\\Dylan\\Desktop\\DH Projects\\Hometown Heros\\MLB_data\\baseballdatabank-master\\core'
os.chdir(path)

mlb_glob = glob.glob(os.path.join(path, "*.csv"))

# mlb_data is a dictioanry of DataFrames acting as the database
mlb_data = {}

for file in mlb_glob:
    # read csv
    
    # Use filename as name of data frame
    table_name = file.split("\\")[-1][:-4]
    data = pd.read_csv(file)
    mlb_data[table_name] = data

# Initializing/configuring (?) geolocator
geolocator = Nominatim(user_agent="hometown_heroes")
geolocator_rate_limited = RateLimiter(geolocator.geocode, min_delay_seconds=1)

#######################################
# Getting location of ballparks
#######################################

df = pd.DataFrame({'park': mlb_data['Parks']['park.name'],
                   'alias': mlb_data['Parks']['park.alias']})

# Cleaner way of doing the below loop - could comve back and use this
df['location'] = df['park'].apply(geolocator_rate_limited)

# STILL TO DO: use the "clean" method above for alias for only rows where 
# NULL location returned for park geocode

# Non-clean way of doing the above
def mygeo(park, alias):
    alias_used = 0
    geoloc = geolocator_rate_limited(park)
    
    # try to geocode the alias if the park name doesnt work
    if geoloc == None:
        geoloc = geolocator_rate_limited(alias)
        alias_used = 1    

    # if the alias doesn't work, assign None to all location attributes
    if geoloc == None:
        lat = None
        long = None
        address = None
    else:
        lat = geoloc.latitude
        long = geoloc.longitude
        address = geoloc.address
    
    return alias_used, lat, long, address

# initializing locaiton list
location_data = []
i = 0
# with exceptions
for key, park, alias in zip(mlb_data['Parks']['park.key'],
                            mlb_data['Parks']['park.name'], 
                            mlb_data['Parks']['park.alias']):
    i = i + 1
    print(park)
    print(alias)
    print("--")
    
    location = mygeo(park, alias)    

    location_data.append([key,
                          park,
                          alias,
                          location[0],
                          location[1],
                          location[2],
                          location[3]]) # park, alias, alias_used, lat, long, address


# joining coordinate data to mlb_data
mlb_data['Parks']['park.key']

location_df = pd.DataFrame(location_data, columns=['key', 'park', 'alias', 
                                                   'alias_used','lat', 
                                                   'long', 'address'])

location_df['key'] = mlb_data['Parks']['park.key']

location_df[['key'] + list(location_df.columns[:-1])]

mlb_data_2 = pd.merge(mlb_data['Parks'],
                        location_df,
                        left_on='park.key',
                        right_on='key').info()

########################################
# Repeat process for hometowns
########################################
home_df = pd.DataFrame({'playerID': mlb_data['People']['playerID'],
                        'birthCountry': mlb_data['People']['birthCountry'], 
                        'birthState': mlb_data['People']['birthState'],	
                        'birthCity': mlb_data['People']['birthCity']})

people_data = mlb_data['People']

# Creating a df with unique city state
home_city_state = sqldf('select distinct birthCity, birthState from people_data')

home_city_state['birthCityState'] = home_city_state['birthCity'] + ', ' + home_city_state['birthState']

home_city_state['geolocation'] = home_city_state['birthCityState'].apply(geolocator_rate_limited)
