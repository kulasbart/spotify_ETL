import requests
import pandas as pd 
import json
from datetime import datetime
import spotify_helpers

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
    username = "e7a487ef0197464391738e0504a7bf9b"
    password = "8370e71d48d74284a3f3d3fac8c4783d"
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

def load(db_config):

    with open('data/db_config.json', 'r') as f:
        db_config = json.load(f)

    engine = create_engine(DATABASE_LOCATION)

    db = mysql.connector.connect(
        host = db_config"",
        user= db_config"",
        password = db_config"",
        database = db_config""
    )

    print(db)
    mycursor = db.cursor()
    #create table skeleton with cursor
    mycursor.execute("CREATE TABLE my_songs (song_name VARCHAR(200), song_artist VARCHAR(200), played_at VARCHAR(200), time_stamp VARCHAR(200))")

    try:
        song_df.to_sql("my_songs", engine, index=False, if_exists='append')
    except Exception as e:
        print(e)
        print("Data already exists in database")
        
    db.close()
    print("Database closed successfully")