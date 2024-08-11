import os
import json
import time
import http.client
import httplib2
import ssl
import random

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError, ResumableUploadError

scopes = ["https://www.googleapis.com/auth/youtube.upload"]


class YoutubeVideoManager:
    def __init__(self,
                 upload_channel_id: str,
                 identity_file: str = "client_secret.json",
                 uploader_identity_file: list[str] = None):
        flow = InstalledAppFlow.from_client_secrets_file(identity_file, scopes)
        credentials = flow.run_local_server()
        self.youtube = build("youtube", "v3", credentials=credentials)

        self.uploader = []
        if not uploader_identity_file:
            self.uploader.append(self.youtube)
        else:
            for identity in uploader_identity_file:
                flow = InstalledAppFlow.from_client_secrets_file(identity, scopes)
                credentials = flow.run_local_server()
                self.uploader.append(build("youtube", "v3", credentials=credentials))

        self.upload_channel_id = upload_channel_id
        self.number_of_upload_video = 0
        self.number_of_uploader = 0


    def upload_video(self,
                     video_file_path: str,
                     title: str,
                     description: str,
                     category_id: str,
                     thumbnail_file_path: str = None,
                     tags: list[str] = []):

        # identity_fileを超えたらエラー
        if self.number_of_uploader > len(self.uploader_identity_file):
            raise Exception("The number of videos uploaded has exceeded the limit.")

        if self.number_of_upload_video % len(self.uploader) == 0:
            self.number_of_uploader += 1

        yt_uploader = self.uploader[self.number_of_uploader]

        print(f"Uploader: {self.number_of_uploader}番目")
        print(f"Video: {self.number_of_upload_video}本目")

        media = MediaFileUpload(video_file_path, resumable=True)
        request = yt_uploader.videos().insert(
            part = "snippet,status",
            body = {
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
            media_body = media,

        )

        response = resumable_upload(request)

        if thumbnail_file_path:
            self.upload_thumbnail(response["id"], thumbnail_file_path)

        return response


    def upload_thumbnail(self,
                         video_id: str,
                         thumbnail_file_path: str):
        media = MediaFileUpload(thumbnail_file_path, chunksize=-1, resumable=True)  # 一括アップロード
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
        request = self.youtube.videos().update(
            part="id,snippet",
            body={
                "id": video_id,
                "snippet": {
                    "channelId": self.upload_channel_id,
                    "title": title,
                    "description": description,
                    "tags": tags,
                    "categoryId": category_id
                }
            }
        )
        response = request.execute()
        print(json.dumps(response, indent=4))

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
                print(f"\rUpload {int(status.progress() * 100)}% complete.   Time: {time.time()-startTime:.2f}", end="")

            if response is not None:
                if 'id' in response:
                    print("Video id '%s' was successfully uploaded." % response['id'])
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

    return response