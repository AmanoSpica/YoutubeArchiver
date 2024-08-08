import os
import json
import time
import asyncio

from dotenv import load_dotenv, find_dotenv

from modules.get_video_data import GetVideoData
from modules.db import DBManager


load_dotenv(find_dotenv())

get_video_data = GetVideoData(
    api_key=os.getenv("YOUTUBE_API_KEY"),
    target_channel_id=os.getenv("TARGET_YOUTUBE_CHANNEL_ID"),
    target_dir="data",
    max_threads=20
)

db = DBManager(
    db_user=os.getenv("DB_USER"),
    db_password=os.getenv("DB_PASSWORD"),
    db_host=os.getenv("DB_HOST"),
    db_port=os.getenv("DB_PORT"),
    db_database=os.getenv("DB_DATABASE")
)


def load_json(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


async def save_to_database(json_data):
    rows = []
    for video in json_data:
        columns = []
        columns.append(video["id"])

        if video["isShorts"]:
            columns.append("shorts")
        elif "liveStreamingDetails" in video:
            columns.append("liveArchive")
        else:
            columns.append("video")

        columns.append(video["snippet"]["title"])
        columns.append(video["snippet"]["description"] if "description" in video["snippet"] else None)
        columns.append(video["snippet"]["publishedAt"].replace("T", " ").replace("Z", ""))

        if "liveStreamingDetails" in video:
            columns.append(video["liveStreamingDetails"]["scheduledStartTime"].replace("T", " ").replace("Z", "") if "scheduledStartTime" in video["liveStreamingDetails"] else None)
            columns.append(video["liveStreamingDetails"]["actualStartTime"].replace("T", " ").replace("Z", "") if "actualStartTime" in video["liveStreamingDetails"] else None)
            columns.append(video["liveStreamingDetails"]["actualEndTime"].replace("T", " ").replace("Z", "") if "actualStartTime" in video["liveStreamingDetails"] else None)
        else:
            columns.append(None)
            columns.append(None)
            columns.append(None)

        columns.append(video["isShorts"])
        columns.append(video["snippet"]["categoryId"] if "categoryId" in video["snippet"] else None)
        columns.append("[" + ",".join([f"'{tag}'" for tag in video["snippet"]["tags"]]) + "]" if "tags" in video["snippet"] else None)

        thumbnails = video["snippet"]["thumbnails"]
        if "maxres" in thumbnails:
            columns.append(thumbnails["maxres"]["url"])
        elif "standard" in thumbnails:
            columns.append(thumbnails["standard"]["url"])
        elif "high" in thumbnails:
            columns.append(thumbnails["high"]["url"])
        elif "medium" in thumbnails:
            columns.append(thumbnails["medium"]["url"])
        else:
            columns.append(thumbnails["default"]["url"])

        columns.append(video["statistics"]["commentCount"] if "commentCount" in video["statistics"] else None)
        columns.append(video["statistics"]["likeCount"] if "likeCount" in video["statistics"] else None)
        columns.append(video["statistics"]["viewCount"])

        rows.append(str(tuple(columns)).replace("None", "NULL"))


    query = f"""
        INSERT INTO TargetVideo
            (id, videoType, title, description, publishedAt,
            liveStreamingDetails_scheduledStartTime, liveStreamingDetails_actualStartTime,
            liveStreamingDetails_actualEndTime, isShorts, categoryId, tags, thumbnails_url,
            commentCount, likeCount, viewCount)
        VALUES
            {",".join(rows)}
        ON DUPLICATE KEY
        UPDATE
            videoType = VALUES(videoType),
            title = VALUES(title),
            description = VALUES(description),
            publishedAt = VALUES(publishedAt),
            liveStreamingDetails_scheduledStartTime = VALUES(liveStreamingDetails_scheduledStartTime),
            liveStreamingDetails_actualStartTime = VALUES(liveStreamingDetails_actualStartTime),
            liveStreamingDetails_actualEndTime = VALUES(liveStreamingDetails_actualEndTime),
            isShorts = VALUES(isShorts),
            categoryId = VALUES(categoryId),
            tags = VALUES(tags),
            thumbnails_url = VALUES(thumbnails_url),
            commentCount = VALUES(commentCount),
            likeCount = VALUES(likeCount),
            viewCount = VALUES(viewCount);
        """


    await db.commit(query)


def main():
    get_video_data.save_video_data()
    video_data = load_json("data/videos.json")
    asyncio.run(save_to_database(video_data))


if __name__ == "__main__":
    print("##### Start #####")
    main()
    print("##### End #####")
