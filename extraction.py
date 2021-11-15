#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sept  5 11:05:16 2021
@author: bartek
"""

import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

#%%

DATABASE_LOCATION = ""
USER_ID = "" # your Spotify username 
TOKEN = "" # your Spotify API token

if __name__ == "__main__":

    headers = {
        "Accept" : "application/json",
        "content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
#%%   
        
#converting yesterdays date unix millisecond timestamps
#want to run this daily and see the songs we played in the last 24hrs 
    
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1) 
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours      
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)
    
    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    
# Extracting only the relevant bits of data from the json object 
     
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
    
    # Prepare a dictionary in order to turn it into a pandas dataframe below       
    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])
    
    print(song_df)
        
        
        
        
        