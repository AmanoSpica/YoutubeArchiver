import os
import json
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv, find_dotenv
import googleapiclient.discovery
import requests

from modules.db import DBManager

load_dotenv(find_dotenv())


db = DBManager(
    db_user=os.getenv("DB_USER"),
    db_password=os.getenv("DB_PASSWORD"),
    db_host=os.getenv("DB_HOST"),
    db_port=os.getenv("DB_PORT"),
    db_database=os.getenv("DB_DATABASE")
)


class GetVideoData:
    def __init__(self,
                 api_key:str,
                 target_channel_id:str,
                 target_dir:str,
                 max_threads:int=5):

        self.api_key = api_key
        self.target_channel_id = target_channel_id
        self.target_dir = target_dir
        self.max_threads = max_threads
        self.youtube =  googleapiclient.discovery.build(
            "youtube", "v3", developerKey=self.api_key)


    def get_uploads_playlist_id(self):
        request = self.youtube.channels().list(
            part="contentDetails",
            id=self.target_channel_id,
            fields="items/contentDetails/relatedPlaylists/uploads"
        )
        response = request.execute()
        return response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


    def get_video_id_in_playlist(self, playlistId):
        video_id_list = []

        request = self.youtube.playlistItems().list(
            part="snippet",
            maxResults=50,
            playlistId=playlistId,
            fields="nextPageToken,items/snippet/resourceId/videoId"
        )

        while request:
            response = request.execute()
            video_id_list.extend(list(map(lambda item: item["snippet"]["resourceId"]["videoId"], response["items"])))
            request = self.youtube.playlistItems().list_next(request, response)

        return video_id_list


    def get_video_items(self, video_id_list):
        video_items = []

        chunk_list = list(chunks(video_id_list, 50))  # max 50 id per request.
        for chunk in chunk_list:
            video_ids = ",".join(chunk)
            request = self.youtube.videos().list(
                part="snippet,statistics,liveStreamingDetails,localizations",
                id=video_ids
            )
            response = request.execute()
            video_items.extend(response["items"])

        return video_items


    def is_youtube_shorts(self, videoId):
        shorts_url = f"https://www.youtube.com/shorts/{videoId}"
        response = requests.get(shorts_url, allow_redirects=True)

        if response.url == shorts_url:
            return True
        elif "watch?v=" in response.url:
            return False
        else:
            return None


    def get_video_type(self, video_items):
        start_time = time.time()
        total = len(video_items)

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            future_to_video = {executor.submit(self.is_youtube_shorts, video["id"]): video for video in video_items}
            for i, future in enumerate(as_completed(future_to_video)):
                video = future_to_video[future]
                try:
                    video["isShorts"] = future.result()
                except Exception as exc:
                    print(f"{video['id']} generated an exception: {exc}")
                end_time = time.time()
                print(f"\rProgress: {i+1}/{total}   time: {int(end_time - start_time)} sec", end="")

        print(f"\r動画タイプを取得しました  time: {int(end_time - start_time)} sec")


    def save_video_data(self):
        uploads_playlist_id = self.get_uploads_playlist_id()
        video_id_list = self.get_video_id_in_playlist(uploads_playlist_id)
        print(f"動画IDを取得しました：{len(video_id_list)}本")
        video_items = self.get_video_items(video_id_list)
        print(f"動画データを取得しました：{len(video_items)}本")
        self.get_video_type(video_items)
        json_save(video_items, os.path.join(self.target_dir, "videos.json"))
        asyncio.run(save_to_database(video_items))


async def save_to_database(json_data):
    chunks_data = list(chunks(json_data, 30))
    for chunk in chunks_data:
        rows = []
        for video in chunk:
            columns = into_str(video)
            rows.append(str(tuple(columns)).replace("None", "NULL"))

        query = f"""
            INSERT INTO TargetVideo
                (id, videoType, title, description, publishedAt,
                liveStreamingDetails_scheduledStartTime, liveStreamingDetails_actualStartTime,
                liveStreamingDetails_actualEndTime, categoryId, tags, thumbnails_url,
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
                categoryId = VALUES(categoryId),
                tags = VALUES(tags),
                thumbnails_url = VALUES(thumbnails_url),
                commentCount = VALUES(commentCount),
                likeCount = VALUES(likeCount),
                viewCount = VALUES(viewCount);
            """

        await db.query(query)


def json_save(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, sort_keys=True, indent=4, ensure_ascii=False)


def load_json(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def into_str(video):
    columns = []
    columns.append(video["id"])

    if video["isShorts"]:
        columns.append("shorts")
    elif "liveStreamingDetails" in video:
        # ライブ配信アーカイブまたはプレミア公開
        columns.append("liveArchive")
    else:
        columns.append("video")

    columns.append(video["snippet"]["title"])
    columns.append(video["snippet"]["description"]
                   if "description" in video["snippet"] else None)
    columns.append(video["snippet"]["publishedAt"].replace("T", " ").replace("Z", ""))

    if "liveStreamingDetails" in video:
        columns.append(video["liveStreamingDetails"]["scheduledStartTime"].replace("T", " ").replace(
            "Z", "") if "scheduledStartTime" in video["liveStreamingDetails"] else None)
        columns.append(video["liveStreamingDetails"]["actualStartTime"].replace("T", " ").replace(
            "Z", "") if "actualStartTime" in video["liveStreamingDetails"] else None)
        columns.append(video["liveStreamingDetails"]["actualEndTime"].replace("T", " ").replace(
            "Z", "") if "actualEndTime" in video["liveStreamingDetails"] else None)
    else:
        columns.append(None)
        columns.append(None)
        columns.append(None)

    columns.append(video["snippet"]["categoryId"]
                   if "categoryId" in video["snippet"] else None)
    columns.append("[" + ",".join([f"'{tag}'" for tag in video["snippet"]
                   ["tags"]]) + "]" if "tags" in video["snippet"] else None)

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

    columns.append(video["statistics"]["commentCount"]
                   if "commentCount" in video["statistics"] else None)
    columns.append(video["statistics"]["likeCount"]
                   if "likeCount" in video["statistics"] else None)
    columns.append(video["statistics"]["viewCount"])
    return columns