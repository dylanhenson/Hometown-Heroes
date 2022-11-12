# -*- coding: utf-8 -*-
"""
Created on Sat Nov 13 22:51:52 2021

@author: Dylan
"""
import pandas as pd
import numpy as np
from pandasql import sqldf
import os
import glob
from geopy.geocoders import Nominatim
from geopy import distance
from geopy.extra.rate_limiter import RateLimiter
import sqlite3
from sqlalchemy import create_engine
from tqdm import tqdm

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


###################################
# Saving dataframes to database
###################################
# SQLAlchemy engine for working with Pandas
engine = create_engine('sqlite:///C:/Users/Dylan/Desktop/DH Projects/Hometown Heros/HometownHeroes.db', echo=False)

# Converting mlb_data dict into sqlite db
for key, df in mlb_data.items(): 
    df.to_sql(key, con=engine, if_exists = 'replace')

# Creating connection to DB (and creating the DB)
conn = engine.connect()

cursor = conn.execute('''
                   SELECT * 
                   FROM Batting
                   ''')
for i in cursor:
    print(i)

# Close connection
conn.close()
engine.dispose()


#######################################
# Getting location of ballparks
#######################################

# Initializing/configuring (?) geolocator
geolocator = Nominatim(user_agent="hometown_heroes")
geolocator_rate_limited = RateLimiter(geolocator.geocode, min_delay_seconds=1.1)

parks_df = pd.DataFrame(mlb_data['Parks'])

parks_df['park_full_location'] = parks_df['park.name'] +\
                                 ', ' + parks_df['city'] +\
                                 ', ' + parks_df['state'] +\
                                 ', ' + parks_df['country']

parks_df['alias_full_location'] = parks_df['park.alias'] +\
                                 ', ' + parks_df['city'] +\
                                 ', ' + parks_df['state'] +\
                                 ', ' + parks_df['country']
                                     

# Cleaner way of doing the below loop - could come back and use this
parks_df['location'] = parks_df['park_full_location'].apply(geolocator_rate_limited)

parks_merged = sqldf('select * FROM parks_df a \
                     left join parks_geo_df b \
                     ON a.[park.key] = b.[key]')

parks_merged = pd.merge(parks_df,
                        parks_geo_df,
                        left_on='park.key',
                        right_on='key')

parks_merged.to_excel('parks_merged.xlsx')

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
                            mlb_data['Parks']['park_full_location'], 
                            mlb_data['Parks']['alias_full_location']):
    i = i + 1
    print(park)
    print(alias)
    print(f"{i}---{key}")
    
    location = mygeo(park, alias)    

    location_data.append([key,
                          park,
                          alias,
                          location[0],
                          location[1],
                          location[2],
                          location[3]]) # park, alias, alias_used, lat, long, address



# Trying above with mapquest -- more accurate

os.chdir('C:\\Users\\Dylan\\Documents\\Projects\\Flex Geocoding\\flex_address_geocode')
import credentials
import json
import requests

def mapquest_geocode(address):
    url = 'http://www.mapquestapi.com/geocoding/v1/address'
    params = {
        'key' : credentials.mapquest_key,
        'location' : address
        }
    
    response = requests.get(url=url, params=params)
    
    data = json.loads(response.text)
    
    #return (data['results'][0]['locations'][0]['latLng']['lat'], data['results'][0]['locations'][0]['latLng']['lng'])
    return data['results'][0]

# Geocoding batch
tqdm.pandas()

mapquest_geocode(parks_df['park_full_location'][0])


parks_df['location'] = parks_df['park_full_location'].progress_apply(mapquest_geocode)

# 8/29/22 left off here - TODO: reverse geocode the coords to ensure they are in correct city

location_df = pd.DataFrame(location_data, columns=['key', 'park', 'alias', 
                                                   'alias_used','lat', 
                                                   'long', 'address'])

location_df['key'] = mlb_data['Parks']['park.key']

location_df[['key'] + list(location_df.columns[:-1])]

# joining coordinate data to mlb_data
parks_location_df = pd.merge(mlb_data['Parks'],
                        location_df,
                        left_on='park.key',
                        right_on='key')

# Replacing results for nans with NaN
parks_location_df['address'][pd.isna(parks_location_df['alias']) 
           & parks_location_df['alias_used'] == 1] = None

parks_location_df.drop(columns=['park.key', 'park.name', 'park.alias'],
                       inplace=True)

####################################################################
# Adding parks_location and hometowns tables to sqlite database
####################################################################

conn = engine.connect()

# Creating Parks_location Table in database
# conn.execute('''DROP TABLE Parks_location''')
conn.execute('''CREATE TABLE Parks_location
                (city TEXT NULL,
                 state TEXT NULL,
                 country TEXT NULL,
                 key TEXT NULL,
                 park TEXT NULL,
                 alias TEXT NULL,
                 alias_used TEXT NULL,
                 latitude TEXT NULL,
                 longitude TEXT NULL,
                 address TEXT NULL);
                ''')

# Inserting data into Hometown_location table
parks_location_df.to_sql('Parks_location', con=engine, if_exists='replace',
                         index=False)

# Close connection
conn.close()
engine.dispose()

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

home_city_state['geolocation'] = home_city_state['birthCityState'].progress_apply(geolocator_rate_limited)

# Extracting latitude nad longitude from "geolocation"

def coords(loc):
    try:
        return [loc.latitude, loc.longitude]
    except:
        return [np.nan, np.nan]

lat, long = [], []
for i in home_city_state['geolocation']:
    lat.append(coords(i)[0])
    long.append(coords(i)[1])

home_city_state['latitude'], home_city_state['longitude'] = lat, long
home_city_state.drop(columns=['geolocation'], inplace=True)

# Replacing results for nans with NaN
home_city_state['latitude'][pd.isna(home_city_state['birthCityState'])] = None
home_city_state['longitude'][pd.isna(home_city_state['birthCityState'])] = None

conn = engine.connect()

# Creating Hometown_location Table in database
# conn.execute('''DROP TABLE Hometown_location''')
conn.execute('''CREATE TABLE Hometown_location
                (birthCity TEXT NULL,
                 birthState TEXT NULL,
                 birthCityState TEXT NULL,
                 latitude TEXT NULL,
                 longitude TEXT NULL);
                ''')

# Inserting data into Hometown_location table
home_city_state.to_sql('Hometown_location', con=engine, if_exists='replace',
                         index=False)

# Close connection
conn.close()
engine.dispose()

##############################################################################
# Finding distances from players' hometowns to all of their home fields
##############################################################################
conn = engine.connect()

parks_geo_df = pd.read_sql('SELECT * FROM Parks_location', conn)
hometown_geo_df = pd.read_sql('SELECT * FROM Hometown_location', conn)

conn.close()
engine.dispose()

# Renaming columns
parks_geo_df = parks_geo_df.rename(columns={'lat': 'park_lat', 'long': 'park_long'})
hometown_geo_df = hometown_geo_df.rename(columns={'latitude': 'home_lat', 'longitude': 'home_long'})


# each player and all parks they've played

appearances_df = pd.DataFrame(mlb_data['Appearances'])
homegames_df = pd.DataFrame(mlb_data['HomeGames'])

# Add coordinates to person df and add new row for each park where they played
person_with_coords = sqldf('SELECT DISTINCT a.playerID, c.yeardID, c.teamID, b.*, e.*\
                           FROM people_data a \
                           LEFT JOIN hometown_geo_df b \
                           ON a.birthCity = b.birthCity \
                           AND a.birthState = b.birthState \
                           LEFT JOIN appearances_df c \
                           ON a.playerID = c.playerID \
                           LEFT JOIN homegames_df d \
                           ON c.teamID = d.[team.key] \
                           AND c.yearID = d.[year.key]\
                           LEFT JOIN parks_geo_df e \
                           ON d.[park.key] = e.key')

# Create a single column with both lat and long coords for park and hometown
person_with_coords['home_coords'] = list(zip(person_with_coords.home_lat, person_with_coords.home_long))
person_with_coords['park_coords'] = list(zip(person_with_coords.park_lat, person_with_coords.park_long))

# Calculate distances from park to hometown
def calc_distance(from_loc, to_loc):
    try:
        return distance.distance(from_loc, to_loc).miles
    except:
        return None

person_with_coords['distance'] = person_with_coords.apply(lambda row: calc_distance(row.home_coords, row.park_coords), axis=1)



# Export final dataframe
person_with_coords.to_excel('player_park_data.xlsx')


##############################################################################
# Ad hoc queries
##############################################################################
conn = engine.connect()

cursor = conn.execute('''
             SELECT * FROM Parks_location
             ''')

for i in enumerate(cursor):
    print(i)
    
conn.close()
engine.dispose()


