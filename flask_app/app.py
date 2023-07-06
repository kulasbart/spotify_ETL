from flask import Flask, request
import requests
import pandas as pd 
import json
import datetime
from spotify_helpers import loadDatabase, getRecentlyPlayed

app = Flask(__name__)

@app.route("/")
def index():
    code = request.args.get('code')
    songs_df, songs_status = getRecentlyPlayed(code)
    songs_html = songs_df.to_html()
    load_status = loadDatabase(songs_df)

    return f"Authorization Code: {code} <br><br>{songs_html}", songs_status

if __name__ == "__main__":
    app.run(host="localhost", port=8080)