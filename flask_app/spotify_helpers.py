import requests
import pandas as pd 
import json
import datetime
import mysql.connector
from sqlalchemy import create_engine

def check_validity(df: pd.DataFrame) -> bool:
    
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

def getRecentlyPlayed(code):
    username = ""
    password = ""
    headers = {"Content-Type" : "application/x-www-form-urlencoded"}
    body = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": "http://localhost:8080"
    }

    # auth2.0
    post = requests.post('https://accounts.spotify.com/api/token', auth=(username, password), headers=headers, data=body).json()

    # send request
    token = post["access_token"]
    url = 'https://api.spotify.com/v1/me/player/recently-played'
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"limit": "50"})
    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    data = r.json()
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
     
    song_dict = {
        "song_name" : song_names,
        "song_artist": artist_names,
        "played_at" : played_at_list,
        "time_stamp" : timestamps
    }

    songs_df = pd.DataFrame(song_dict, columns = ["song_name", "song_artist", "played_at", "time_stamp"])
    print(songs_df)

    if check_validity(songs_df):
        print("Data validity passed: proceed to load")
    print(songs_df)

    return songs_df, 200

def loadDatabase(song_df, db_config='../db/db_config.json'):

    with open(db_config, 'r') as f:
        db_config = json.load(f)

    # enter db location
    database_location = 'mysql+mysqlconnector://<username>:<password>@<host>:<port>/<database>'
    engine = create_engine(database_location)

    db = mysql.connector.connect(
        host = db_config['host'],
        user= db_config['user'],
        password = db_config['password'],
        database = db_config['db_name']
    )

    print(db)
    cursor = db.cursor()

    #create table skeleton with cursor
    cursor.execute("CREATE TABLE recently_played_songs (song_name VARCHAR(200), song_artist VARCHAR(200), played_at VARCHAR(200), time_stamp VARCHAR(200))")

    try:
        song_df.to_sql("recently_played_songs", engine, index=False, if_exists='append')
    except Exception as e:
        print(e)
        print("Data already exists in database")
    
    cursor.close()
    db.close()
    print("Database closed successfully")

    return 200