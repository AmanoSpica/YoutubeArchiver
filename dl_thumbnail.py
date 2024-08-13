import os
import asyncio
import urllib.request
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from modules.db import DBManager


load_dotenv(find_dotenv())

db = DBManager(
    db_user=os.getenv("DB_USER"),
    db_password=os.getenv("DB_PASSWORD"),
    db_host=os.getenv("DB_HOST"),
    db_port=os.getenv("DB_PORT"),
    db_database=os.getenv("DB_DATABASE")
)


def download_youtube_thumbnail(filename, target_dir, url):
    file_name = f'{target_dir}/{filename}.jpg'
    print(f"[urllib.request] Downloading thumbnail: {file_name}")
    urllib.request.urlretrieve(url, file_name)
    return file_name

if __name__ == "__main__":
    thumbnails = asyncio.run(
        db.query("SELECT title, thumbnails_url FROM TargetVideo ORDER BY publishedAt ASC;")
    )
    print(thumbnails)
    for i, thumb in thumbnails.iterrows():
        if i > 509:
            print(thumb)
            title = thumb['title'][:12].replace("/","").replace("|","")
            download_youtube_thumbnail(f"{i}", "img", thumb["thumbnails_url"])