from cred_refresh import Refresh
from secrets import server
import pandas as pd
import requests
from datetime import datetime, timedelta
import pyodbc
from sqlalchemy import create_engine
import urllib

def check_if_valid_data(df) -> bool:
    if df.empty:
        print('No songs downloaded')
        quit()

    if pd.Series(df['primary_key']).is_unique:
        pass
    else:
        raise Exception('Primary Key Check Violated')

    if df.isnull().values.any():
        raise Exception('Null value found')

    yesterday = datetime.now() - timedelta(hours=20)
    timestamps = df['primary_key']
    for timestamp in timestamps:
        played_time = datetime.strptime(timestamp[:19], '%Y-%m-%dT%H:%M:%S')
        if played_time < yesterday:
            raise Exception('At least one song is not from the past 24 hrs')

    return True

def run_spotify_etl():
    # define components of our connection string
    driver = '{ODBC DRIVER 17 for SQL Server}'
    # server = 'localhost\server_name'
    database = 'SpotifySongs'

    # define our connection string
    connection_string = '''
    Driver={driver};
    Server={server};
    Database={database};
    Trusted_Connection=yes;
    '''.format(
        driver=driver,
        server=server,
        database=database
    )

    # connect to database
    conn = pyodbc.connect(connection_string)

    # create the connection cursor
    cursor = conn.cursor()

    # create the table if it doesn't exist
    create_table = '''
    IF (
        NOT EXISTS (
            SELECT * 
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'my_played_tracks'
            )
        )
    BEGIN
        CREATE TABLE my_played_tracks (
            primary_key VARCHAR(200) NOT NULL PRIMARY KEY,
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            timestamp VARCHAR(200) 
        )
    END;
    '''

    cursor.execute(create_table)
    conn.commit()
    print('Opened database successfully')

    print('Refreshing token')
    refreshCaller = Refresh()
    token = refreshCaller.refresh()

    headers = {
        'Accept' : 'application/json',
        'Content-Type' : 'application/json',
        'Authorization' : 'Bearer {token}'.format(token=token)
    }

    print('Extracting data from Spotify')
    # 'Now' is in GMT timezone in the Spotify timestamps, which is +4hrs from my EDT timezone
    today = datetime.now() + timedelta(hours=4)
    yesterday = today - timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    r = requests.get('https://api.spotify.com/v1/me/player/recently-played?after={time}'.format(time=yesterday_unix_timestamp), headers=headers)

    data = r.json()

    primary_key = []
    song_names = []
    artist_names = []
    timestamps = []

    print('Processing song data')
    for song in data['items']:
        primary_key.append(song['played_at'])
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['artists'][0]['name'])
        timestamps.append(song['played_at'][0:10])

    song_dict = {
        'primary_key' : primary_key,
        'song_name' : song_names,
        'artist_name' : artist_names,
        'timestamp' : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ['primary_key', 'song_name', 'artist_name', 'timestamp'])

    if check_if_valid_data(song_df):
        print('Data validated')
    print('Loading')

    try:
        quoted = urllib.parse.quote_plus(connection_string)
        engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
        song_df.to_sql('my_played_tracks', engine, index=False, if_exists='append')
    except:
        print('Data already exists in database')

    conn.close()
    print('Database closed successfully')