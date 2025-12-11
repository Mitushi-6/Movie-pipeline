from sqlite3 import Cursor
import pandas as pd
import requests
import mysql.connector
import datetime
from datetime import datetime
import math
import time
import os
user="root"
host="localhost"
password="hihello12!"
database="movie_pipeline"
port=3306
table_name="movies_data"
batch_size = 100
def extra_details(title):
    url = f"http://www.omdbapi.com/?t={title}&&apikey=aedebc2a"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get('Response') == 'True':
                return data.get('Director'), data.get('Released')
    except:
        pass
    return None, None

movies=pd.read_csv("movies.csv")
movies = movies.head(200)
if 'director' not in movies.columns:
    movies['director'] = None
if 'release_date' not in movies.columns:
    movies['release_date'] = None
movies=movies[["movieId","title","genres","director","release_date"]]
ratings=pd.read_csv("ratings.csv")
ratings=ratings[["movieId","rating"]]

ratings=ratings.sort_values(by="movieId").groupby("movieId").mean()
data=pd.merge(movies,ratings,on="movieId")

def parse_date(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    if str(x).strip().upper() == "N/A" or str(x).strip() == "":
        return None
    x = str(x).strip()
    formats = [
        "%d-%b-%y",    
        "%d %b %Y",    
        "%d-%b-%Y",    
        "%Y-%m-%d"
    ]
    for fmt in formats:
        try:
            return pd.to_datetime(x, format=fmt).date()
        except:
            continue
    return None

data["release_date"] = data["release_date"].apply(parse_date)
print(data)
conn = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database,
    port=port,
     auth_plugin='mysql_native_password' 
)
cursor = conn.cursor()
insert_sql = f"""
INSERT INTO {table_name} (movieId, title, genres, director, release_date, rating)
VALUES (%s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    title = VALUES(title),
    genres = VALUES(genres),
    director = VALUES(director),
    release_date = VALUES(release_date),
    rating = VALUES(rating)
"""
print("Preparing data for insert...")
rows = []
for _, row in data.iterrows():
    rows.append((
        int(row["movieId"]),
        str(row["title"]),
        str(row["genres"]) if pd.notna(row["genres"]) else None,
        str(row["director"]) if pd.notna(row["director"]) else None,
        row["release_date"],
        float(row["rating"]) if pd.notna(row["rating"]) else None
    ))
cursor.executemany(insert_sql, rows)
conn.commit()
print(f"Inserted {len(rows)} rows into {table_name} successfully!")
cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
count = cursor.fetchone()[0]
print(f"Verification: Table now has {count} rows")
cursor.close()
conn.close()  
#print(ratings)
#print(movies)
'''split= movies['title'].str.rsplit("(",n=1,expand=True)
if split.shape[1] == 1:
    split[1] = None
split.columns=['title','year']
movies['title'] = split['title'].str.strip()
movies['year'] = split['year'].str.replace(")", "").str.strip()'''
#movies['year'] = pd.to_numeric(movies['year'], errors='coerce').astype('Int64')
'''while True:
    process = movies[movies['director'].isna() | movies['release_date'].isna()]
    if process.empty:
        print("All movies have director and release date filled.")
        break
    batch = process.head(batch_size)
    for idx, row in batch.iterrows():
        director, release_date = extra_details(row['title'])
        if director:
            movies.at[idx, 'director'] = director
        if release_date:
            movies.at[idx, 'release_date'] = release_date
        time.sleep(1)
    movies.to_csv("movies.csv",index=False)'''
'''ratings=ratings.sort_values(by="movieId").groupby("movieId").mean()
data=pd.merge(movies,ratings,on="movieId")
print(data)
print(ratings)
print(movies)'''



'''
with open("movies.csv","r") as file:
    reader=csv.DictReader(file)
    for row in reader:
        title=row["title"]
        if "(" in title and title[-5:-1].isdigit():
            movie,year=title.rsplit("(", 1)
            movie=movie.strip()
            year=year.strip(")")
            rating=0
        else:
            movie=title.strip()
            year="No Year"

        movies.append({
            "movieId": row["movieId"],
            "title": movie,
            "year": year,
            "rating": rating
        })
ratings = {}
with open("ratings.csv","r") as file:
    reader=csv.DictReader(file)
    for row in reader:
        movie_id=row["movieId"]
        rating=float(row["rating"])
        if movie_id in ratings:
            ratings[movie_id].append(rating)
        else:
            ratings[movie_id]=[rating]
print(f"{'Movie':40} {'Year':10} {'Rating'}")
print("-" * 65)
for m in movies:
    movie_id = m["movieId"]
    movie = m["title"]
    year = m["year"]

    if movie_id in ratings:
        rratings = ", ".join([str(r) for r in ratings[movie_id]])
        print(f"{movie[:38]:40} {year:10} {rratings}")
    else:
        print(f"{movie[:38]:40} {year:10} {'No Rating'}")
'''