import os
import json

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import googleapiclient.errors

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

        self.number_of_upload_video += 1
        if self.number_of_uploader > len(self.uploader):
            raise Exception("The number of videos uploaded has exceeded the limit.")

        if self.number_of_upload_video % len(self.uploader) == 0:
            self.number_of_uploader += 1

        yt_uploader = self.uploader[self.number_of_uploader]

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
            media_body = MediaFileUpload(video_file_path),

        )
        response = request.execute()

        if thumbnail_file_path:
            self.upload_thumbnail(response["id"], thumbnail_file_path)

        return response


    def upload_thumbnail(self,
                         video_id: str,
                         thumbnail_file_path: str):
        request = self.youtube.thumbnails().set(
            media_body = MediaFileUpload(thumbnail_file_path),
            videoId = video_id
        )
        response = request.execute()

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