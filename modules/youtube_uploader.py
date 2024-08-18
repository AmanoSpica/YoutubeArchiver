import os
import json
import time
import http.client
import httplib2
import ssl
import random
import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


import requests
from dotenv import load_dotenv, find_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

from modules.db import DBManager

load_dotenv(find_dotenv())


db = DBManager(
    db_user=os.getenv("DB_USER"),
    db_password=os.getenv("DB_PASSWORD"),
    db_host=os.getenv("DB_HOST"),
    db_port=os.getenv("DB_PORT"),
    db_database=os.getenv("DB_DATABASE")
)


scopes = ["https://www.googleapis.com/auth/youtube.upload",
          "https://www.googleapis.com/auth/youtube.force-ssl"]


def googleapiclient_login(identity_file: str, port: int = 8080):
    print(f"\nLogin with {identity_file}\nRunning Login on port {port}")
    flow = InstalledAppFlow.from_client_secrets_file(identity_file, scopes)
    credentials = flow.run_local_server(port=port)
    youtube = build("youtube", "v3", credentials=credentials)

    return youtube


class YoutubeVideoManager:
    def __init__(self,
                 api_key: str,
                 target_channel_id: str,
                 upload_channel_id: str,
                 max_threads: int = 5):

        self.target_channel_id = target_channel_id
        self.upload_channel_id = upload_channel_id
        self.max_threads = max_threads

        self.youtube_dataSystem = build("youtube", "v3", developerKey=api_key)

        client_data = asyncio.run(db.query("SELECT * FROM QuotaData WHERE name = 'YoutubeArchiver';")).iloc[0]
        if client_data.empty:
            raise Exception("No client data found in the database.")

        self.youtube = googleapiclient_login(client_data["identityFile"])

        self.uploader = {}
        uploader_data = asyncio.run(db.query(
            "SELECT * FROM QuotaData WHERE name LIKE 'YoutubeArchiver-uploader%' ORDER BY name ASC;"))
        print(uploader_data)
        for d in uploader_data.itertuples():
            print(d.name)
            port = 8000 + int(d.name[-2:]) - 1
            self.uploader[d.name] = (d.identityFile, port)

    async def _quota(self, name: str, value: int):
        data = (await db.query(
            f"SELECT quota FROM QuotaData WHERE name = '{name}';")).iloc[0]
        if data.empty:
            raise Exception(f"No client data found in the database: {name}")
        if not data["quota"] + value < 10000:
            raise Exception(f"Quota exceeded: {name}")
        await db.query(
            f"UPDATE QuotaData SET quota = quota + {value} WHERE name = '{name}';")
        return

    def get_uploads_playlist_id(self):
        asyncio.run(self._quota("YoutubeArchiver-DataSystem", 1))
        request = self.youtube_dataSystem.channels().list(
            part="contentDetails",
            id=self.target_channel_id,
            fields="items/contentDetails/relatedPlaylists/uploads"
        )
        response = request.execute()
        return response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    def get_video_id_in_playlist(self, playlistId):
        video_id_list = []
        request = self.youtube_dataSystem.playlistItems().list(
            part="snippet",
            maxResults=50,
            playlistId=playlistId,
            fields="nextPageToken,items/snippet/resourceId/videoId"
        )

        while request:
            asyncio.run(self._quota("YoutubeArchiver-DataSystem", 1))
            response = request.execute()
            video_id_list.extend(list(
                map(lambda item: item["snippet"]["resourceId"]["videoId"], response["items"])))
            request = self.youtube_dataSystem.playlistItems().list_next(request, response)

        return video_id_list

    def get_video_items(self, video_id_list):
        video_items = []

        chunk_list = list(chunks(video_id_list, 50))  # max 50 id per request.
        for chunk in chunk_list:
            asyncio.run(self._quota("YoutubeArchiver-DataSystem", 1))
            video_ids = ",".join(chunk)
            request = self.youtube_dataSystem.videos().list(
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
            future_to_video = {executor.submit(
                self.is_youtube_shorts, video["id"]): video for video in video_items}
            for i, future in enumerate(as_completed(future_to_video)):
                video = future_to_video[future]
                try:
                    video["isShorts"] = future.result()
                except Exception as exc:
                    print(f"{video['id']} generated an exception: {exc}")
                end_time = time.time()
                print(
                    f"\rProgress: {i+1}/{total}   time: {int(end_time - start_time)} sec", end="")

        print(f"\r動画タイプを取得しました  time: {int(end_time - start_time)} sec")


    def save_video_data(self):
        uploads_playlist_id = self.get_uploads_playlist_id()
        video_id_list = self.get_video_id_in_playlist(uploads_playlist_id)
        print(f"動画IDを取得しました：{len(video_id_list)}本")
        video_items = self.get_video_items(video_id_list)
        print(f"動画データを取得しました：{len(video_items)}本")
        self.get_video_type(video_items)
        json_save(video_items, os.path.join("data", "videos.json"))
        asyncio.run(self._save_database(video_items))


    async def _save_database(self, json_data):
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

            await db.query(query.replace("Ninomae Ina'nis", "Ninomae Ina’nis"))


    async def _select_uploader(self, value: int):
        while True:
            uploader_data = (await db.query(
                f"SELECT * FROM QuotaData WHERE name LIKE 'YoutubeArchiver-uploader%' AND 10000 > quota + {value} ORDER BY name ASC LIMIT 1;")).iloc[0]
            if uploader_data is None:
                print(f"\rNo uploader available. Waiting for 15 minutes.  {datetime.now()}", end="")
                time.sleep(60*15)
            else:
                await db.query(
                    f"UPDATE QuotaData SET quota = quota + {value} WHERE name = '{uploader_data['name']}';")
                break

        if type(self.uploader[uploader_data["name"]]) == tuple:
            self.uploader[uploader_data["name"]] = googleapiclient_login(self.uploader[uploader_data["name"]][0], self.uploader[uploader_data["name"]][1])

        return self.uploader[uploader_data["name"]]


    def upload_video(self,
                     video_file_path: str,
                     title: str,
                     description: str,
                     category_id: str,
                     thumbnail_file_path: str = None,
                     tags: list[str] = []):

        yt_uploader = asyncio.run(self._select_uploader(1600))

        media = MediaFileUpload(video_file_path, resumable=True)
        request = yt_uploader.videos().insert(
            part="snippet,status",
            body={
                "snippet": {
                    "channelId": self.upload_channel_id,
                    "title": title,
                    "description": description,
                    "tags": tags,
                    "categoryId": category_id,
                    "defaultLanguage": "ja_JP",
                    "defaultAudioLanguage": "ja_JP"
                },
                "status": {
                    "uploadStatus": "uploaded",
                    "privacyStatus": "private",
                    "license": "youtube",
                    "embeddable": "true",
                    "selfDeclaredMadeForKids": "false"
                }
            },
            media_body=media,

        )

        response = resumable_upload(request)

        if thumbnail_file_path:
            self.upload_thumbnail(response["id"], thumbnail_file_path)

        return response


    def upload_thumbnail(self,
                         video_id: str,
                         thumbnail_file_path: str):
        asyncio.run(self._quota("YoutubeArchiver", 50))
        media = MediaFileUpload(thumbnail_file_path,
                                chunksize=-1, resumable=True)  # 一括アップロード
        request = self.youtube.thumbnails().set(
            media_body=media,
            videoId=video_id
        )
        response = resumable_upload(request)

        return response


    def edit_video(self,
                   video_id: str,
                   title: str,
                   description: str,
                   category_id: str,
                   tags: list[str] = []):
        asyncio.run(self._quota("YoutubeArchiver-DataSystem", 50))
        request = self.youtube_dataSystem.videos().update(
            part="id,snippet",
            body={
                "id": video_id,
                "snippet": {
                    "channelId": self.upload_channel_id,
                    "title": title,
                    "description": description,
                    "tags": tags,
                    "categoryId": category_id,
                    "defaultLanguage": "ja_JP",
                    "defaultAudioLanguage": "ja_JP"
                }
            }
        )
        response = request.execute()

        return response



httplib2.RETRIES = 1
MAX_RETRIES = 10
RETRIABLE_EXCEPTIONS = (httplib2.HttpLib2Error,
                        IOError,
                        http.client.NotConnected,
                        http.client.IncompleteRead,
                        http.client.ImproperConnectionState,
                        http.client.CannotSendRequest,
                        http.client.CannotSendHeader,
                        http.client.ResponseNotReady,
                        http.client.BadStatusLine,
                        ssl.SSLWantWriteError)
RETRIABLE_STATUS_CODES = [500, 502, 503, 504]


def resumable_upload(insert_request):
    startTime = time.time()
    response = None
    error = None
    retry = 0
    previous_progress = 0
    print("Uploading file...")
    while response is None:
        try:
            status, response = insert_request.next_chunk()
            if status:
                print(
                    f"\rUpload {int(status.progress() * 100)}% complete.   Time: {time.time()-startTime:.2f}", end="")

            if response is not None:
                if 'id' in response:
                    print("\rVideo id '%s' was successfully uploaded." %
                          response['id'])
                else:
                    print("Thumbnail was successfully uploaded.")
        except HttpError as e:
            if e.resp.status in RETRIABLE_STATUS_CODES:
                error = "A retriable HTTP error %d occurred:\n%s" % \
                        (e.resp.status, e.content)
            else:
                raise
        except RETRIABLE_EXCEPTIONS as e:
            error = "A retriable error occurred: %s" % e
        if error is not None:
            print(error)
            retry += 1
            if retry > MAX_RETRIES:
                exit("No longer attempting to retry.")
            max_sleep = 2 ** retry
            sleep_seconds = random.random() * max_sleep
            print("Sleeping %f seconds and then retrying..." % sleep_seconds)
            time.sleep(sleep_seconds)
            error = None

    return response


def json_save(data, filename):
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))
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
    columns.append(video["snippet"]["publishedAt"].replace(
        "T", " ").replace("Z", ""))

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
