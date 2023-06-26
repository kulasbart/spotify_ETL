#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import mysql.connector

#%%

DATABASE_LOCATION = ""
USER_ID = "" # your Spotify username 
# Generate token here https://developer.spotify.com/console/get-recently-played/?limit=20&after=1484811043508&before=
TOKEN = "" # insert token

def check_if_valid_data(df: pd.DataFrame) -> bool:
    
    if df.empty:
        print ("No songs downloaded. Finishing Execution")
        return False
    
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Duplicates detected. Primary Key check is violated.")
        
    if df.isnull().values.any():
        raise Exception("Null value found")
    
    #Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    # timestamps = df["timestamp"].tolist()
    # for timestamp in timestamps:
    #     if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
    #         raise Exception("At least one of the returned songs does not have a yesterday's timestamp")
    # return True


# use POST
# basic auth header / header / body information
if __name__ == "__main__":

    username = ""
    password = ""

    headers = {
        "Content-Type" : "application/x-www-form-urlencoded"
    }

    body = {
        "grant_type": "authorization_code",
        "code": "AQBB9hq4vPoiQJsNzzHLeVvr1AmNxn3OBizhCNtLWtv34sNr17x7_Dbbzt0umoybHCrjFsvZkyDs9BRTEx9Z2OrtrFtKcMSOrdczwCpyfe6gPDgdNF4tyU8NuV-Vo9KK7JZUoZu0pPXi75z1wQWnQtPFTP0xq6_woHL5GQQR2FDDKU8k6T3mFff_UPWte4dkDzKcTw",
        "redirect_uri": "http://localhost:8080"
    }
        
    # converting yesterdays date unix millisecond timestamps
    # checks for songs played in last 24 h
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1) 
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
     
    post = requests.post('https://accounts.spotify.com/api/token', auth=(username, password), headers=headers, data=body).json()
    print(post)

    # request
    token = post["access_token"]
    url = 'https://api.spotify.com/v1/me/player/recently-played'
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"})

    data = r.json()
    print(data)

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
        "song_artist": artist_names,
        "played_at" : played_at_list,
        "time_stamp" : timestamps
    
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "song_artist", "played_at", "time_stamp"])

    print(song_df)

#%%  
#Validate
    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")
    
    print(song_df)
    
#%%        
#Loading

engine = create_engine(DATABASE_LOCATION)

#construct cursor object

db = mysql.connector.connect(  
  host="",
  user="",
  password="",
  database=""
)

print(db)

mycursor = db.cursor()


#create table skeleton with cursor
#mycursor.execute("CREATE TABLE my_songs (song_name VARCHAR(200), song_artist VARCHAR(200), played_at VARCHAR(200), time_stamp VARCHAR(200))")

try:
    song_df.to_sql("my_songs", engine, index=False, if_exists='append')
except Exception as e:
    print(e)
    print("Data already exists in database")
    
db.close()
print("Database closed successfully")