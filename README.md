![Spotify ETL](./figures/Spotify_ETL.svg)

Made with [draw.io](https://drive.google.com/file/d/1GGBgKkbAsrv0-ypzIxPXeZlYJxb4aL-O/view?usp=sharing)

## Spotify ETL Using Python, Apache Airflow, and MS SQL Server
The Spotify API documentation is very well written and can be used as a walkthrough for a wide variety of project applications. In this project I set up a daily extraction of my recently played tracks using Python for the data processing and Apache Airflow as the scheduler. The resulting data is appended to a database on a local Microsoft SQL Server.

Here is the resulting table:

![Pasted image 20220419223941.png](./figures/Pasted%20image%2020220419223941.png)

## Data Extraction
```python
headers = {  
    'Accept' : 'application/json',  
    'Content-Type' : 'application/json',  
    'Authorization' : 'Bearer {token}'.format(token=token)  
}  
  
today = datetime.now() + timedelta(hours=4)  
yesterday = today - timedelta(days=1)  
yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000  
r = requests.get('https://api.spotify.com/v1/me/player/recently-played?after={time}'.format(time=yesterday_unix_timestamp), headers=headers)  
  
data = r.json()
```

## Data Transformation and Validation
```python
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
```

```python
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
```

## Loading Data into MS SQL Server
Initiating table:
```python
driver = '{ODBC DRIVER 17 for SQL Server}'  
server = 'localhost\server_name'  
database = 'db_name'  
  
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
  
conn = pyodbc.connect(connection_string)  
cursor = conn.cursor()  

create_table = '''  
IF (  
    NOT EXISTS (
		SELECT *
		FROM INFORMATION_SCHEMA.TABLES  
		WHERE TABLE_NAME = 'my_played_tracks'        
		)    
)BEGIN  
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
```

Loading Data:
```python
quoted = urllib.parse.quote_plus(connection_string)  
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))  
song_df.to_sql('my_played_tracks', engine, index=False, if_exists='append')

conn.close()
```

## Scheduling with Apache Airflow
Airflow was installed locally for this project by following the Apache [documentation](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html).

```bash
airflow webserver -p 8080
```

```bash
airflow scheduler
```

Airflow Windows: https://towardsdatascience.com/run-apache-airflow-on-windows-10-without-docker-3c5754bb98b4

## OAuth 2.0 Authorization 
In order to extract the song information from Spotify's API we need an 'Access Token'. This can be acquired manually from [Spotify](https://developer.spotify.com/console/get-recently-played/). If you only require temporary access, you can skip this next part, however a code would have to be manually produced and copied into the program every time it runs.

In order to programatically request an 'Access Token', authorization must be granted through the [OAuth 2.0 framework](https://developer.spotify.com/documentation/general/guides/authorization/). This program follows the [Authorization Code Flow](https://developer.spotify.com/documentation/general/guides/authorization/code-flow/). 3 pieces of information are required for the end product:
- client_id
- client_secret
- refresh_token

Getting these variables involves creating a Spotify Dev App and manually authorizing the app in a browser.

### Spotify Dev App
Spotify has an [App Guide](https://developer.spotify.com/documentation/general/guides/authorization/app-settings/) that describes how to create an App. The 'client_id' and 'client_secret' are both at the top of the app page. 

Remember to add a 'Redirect URI' to the app (I used https://localhost:8000/callback). It doesn't have to be a real website, it is just required to have a trusted destination for Spotify to send the 'authorization code' required for the next step.

### One-Time Manual User Authorization
The rest of this setup will follow the [Authorization Code Flow](https://developer.spotify.com/documentation/general/guides/authorization/code-flow/) document. 

The goal is to acquire a 'Refresh Token' which will allow us to request and receive a new 'Access Token' programatically. In order to get this token a 'GET request' must be sent to the '/authorize' endpoint. This GET request can be as simple as a browser url:

```
https://accounts.spotify.com/authorize?client_id=<client_id_from_app>&scope=user-read-recently-played&response_type=code&redirect_uri=https%3A%2F%2Flocalhost%3A8888%2Fcallback
```

Parameters:
- client_id: ID from your newly created app
- response_type: code
- redirect_uri: The url added to the app settings, encoded [here](https://www.urlencoder.org/)
- scope: user-read-recently-played (or any [other scopes])
	- Use ''%20' as a space to separate multiple scopes

After building this URL, authorization is requested:

![Pasted image 20220419213634_blur.png](./figures/Pasted%20image%2020220419213634_blur.png)

After the redirect, the 'authorization code' is appended to the url in the browser:

![Screenshot 2022-04-19 205439_short.png](./figures/Screenshot%202022-04-19%20205439_short.png)

Run a `curl` command with the code to get an Access Token and a **Refresh Token**

```bash
curl -H "Authorization: Basic <base64 encoded client_id:secret_id>" \
	-H "Content-Type: application/x-www-form-urlencoded" \
	-d grant_type=authorization_code \
	-d code=AQBicXEN4K...qU_2-Pr0Q \
	-d redirect_uri=https%3A%2F%2Flocalhost%3A8888%2Fcallback \
	https://accounts.spotify.com/api/token
```

Parameters:
- Authorization: Basic \<base64 encoded client_id:secret_id>
	- Ex. paste your 'client_id:secret_id' string into [here](https://www.base64encode.org/)
- Content-Type: application/x-www-form-urlencoded
- grant_type=authorization_code
- code=AQBicXEN4Ks...WYAqU_2-Pr0Q
	- Given by Spotify in the redirect URL
- redirect_uri=https%3A%2F%2Flocalhost%3A8000%2Fcallback
	- Only used for verification

The response should look like this:
```json
{
   "access_token": "NgCXRK...MzYjw",
   "token_type": "Bearer",
   "scope": "user-read-private user-read-email",
   "expires_in": 3600,
   "refresh_token": "NgAagA...Um_SHo"
}
```

The 'access_token' can be used to authenticate connection the API, however it expires in 1 hour. The 'refresh_token', however, can be used along with the base64 encoded app credentials to get another 1 hour 'access_token'--giving unlimited access.

The code below is used to acquire a new 'access_token' at each run:

```python
def refresh(self):  
  
    query = 'https://accounts.spotify.com/api/token'  
  
    response = requests.post(query,  
                             data={'grant_type': 'refresh_token',  
                                   'refresh_token': refresh_token},  
                             headers={'Authorization': 'Basic ' + base_64})  
    return response.json()['access_token']
```

### Helpful Links
OAuth 2: https://developer.spotify.com/documentation/general/guides/authorization/
App Auth: https://developer.spotify.com/documentation/general/guides/authorization/code-flow/
App setup: https://developer.spotify.com/documentation/general/guides/authorization/app-settings/
Using App Access: https://developer.spotify.com/documentation/general/guides/authorization/use-access-token/

For setting the scope of the returned data:
Scopes guide: https://developer.spotify.com/documentation/general/guides/authorization/scopes/
Recently Played scope: https://developer.spotify.com/documentation/web-api/reference/#/operations/get-the-users-currently-playing-track

Others Implementations:
Implementation: https://github.com/kylepw/spotify-api-auth-examples
YT Url hacking: https://www.youtube.com/watch?v=yAXoOolPvjU