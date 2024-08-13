import os
import asyncio
import datetime

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

def format_video_info(video_data: dict):
    title = video_data["title"]
    video_info = ""
    if video_data["videoType"] == "shorts":
        video_info += "【#Shorts】\n"
        title += "  #Shorts"
    elif video_data["videoType"] == "liveArchive":
        video_info += "【配信アーカイブ または プレミア公開 動画】\n"
        video_info += f"配信・公開予定日時: {format_datetime(video_data['liveStreamingDetails_scheduledStartTime'])}\n" if video_data["liveStreamingDetails_scheduledStartTime"] is not None else "配信・公開予定日時: [指定なし]\n"
        video_info += f"配信・公開開始日時: {format_datetime(video_data['liveStreamingDetails_actualStartTime'])}\n"
        video_info += f"配信・公開終了日時: {format_datetime(video_data['liveStreamingDetails_actualEndTime'])}\n"
    elif video_data["videoType"] == "video":
        video_info += "【通常動画】\n"
    else:
        raise "Error: videoType is invalid."

    video_info += f"投稿日時: {format_datetime(video_data['publishedAt'])}\n"
    video_info += f"再生回数: {insert_comma(video_data['viewCount'])} 回\n" if video_data["viewCount"] is not None else "再生回数: [非公開]\n"
    video_info += f"高評価数: {insert_comma(video_data['likeCount'])} 件\n" if video_data["likeCount"] is not None else "高評価数: [非公開]\n"
    video_info += f"コメント数: {insert_comma(video_data['commentCount'])} 件\n" if video_data["commentCount"] is not None else "コメント数: [コメント無効]\n"
    video_info += f"※ データは取得時点({datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')})のものです。\n\nこの動画は「Aqua Ch. 湊あくあ」さんの公式チャンネルから取得したアーカイブ動画です。"
    video_info = f"{'#'*20}\n\n{video_info}\n\n{'#'*20}\n\n\n\n{video_data['description']}"

    title = title.replace("u3000", "　")
    video_info = video_info.replace("u3000", "　")
    return title, video_info


def get_description(id: str):
    video_data = asyncio.run(
        db.query(
            f"SELECT * FROM TargetVideo WHERE id = '{id}';"
        )
    )
    if video_data.empty:
        print("Error: Video data is not found.")
        return None, None
    else:
        title, description = format_video_info(video_data.iloc[0])
        print(f"title: {title}")
        return title, description


def format_datetime(datetime_str):
    if datetime_str is None:
        return "[非公開]"
    if type(datetime_str) == str:
        return datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S").strftime("%Y/%m/%d %H:%M:%S")
    else:
        return datetime_str.strftime("%Y/%m/%d %H:%M:%S")
def insert_comma(text: str) -> str:
    # 3文字ごとにカンマを挿入
    text = int(text)
    return str("{:,}".format(text))

if __name__ == "__main__":
    while True:
        id = input("Enter the video ID: ")
        if id == "":
            break
        title, description = get_description(id)
        with open("video_description.txt", "w", encoding="utf-8") as f:
            f.write(description)