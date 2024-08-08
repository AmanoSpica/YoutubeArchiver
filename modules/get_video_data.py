import os
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv, find_dotenv
import googleapiclient.discovery
import requests

load_dotenv(find_dotenv())

api_service_name = "youtube"
api_version = "v3"

DEVELOPER_KEY = os.getenv('YOUTUBE_API_KEY')
max_threads = int(os.getenv('MAX_THREADS'))


def json_save(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, sort_keys=True, indent=4, ensure_ascii=False)


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def get_authenticated_service():
    """Get authenticated service"""
    return googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=DEVELOPER_KEY)


youtube = get_authenticated_service()



def get_uploads_playlist_id(channelId):
    request = youtube.channels().list(
        part="contentDetails",
        id=channelId,
        fields="items/contentDetails/relatedPlaylists/uploads"
    )
    response = request.execute()
    return response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


def get_video_id_in_playlist(playlistId):
    video_id_list = []

    request = youtube.playlistItems().list(
        part="snippet",
        maxResults=50,
        playlistId=playlistId,
        fields="nextPageToken,items/snippet/resourceId/videoId"
    )

    while request:
        response = request.execute()
        video_id_list.extend(list(map(lambda item: item["snippet"]["resourceId"]["videoId"], response["items"])))
        request = youtube.playlistItems().list_next(request, response)

    return video_id_list


def get_video_items(video_id_list):
    video_items = []

    chunk_list = list(chunks(video_id_list, 50))  # max 50 id per request.
    for chunk in chunk_list:
        video_ids = ",".join(chunk)
        request = youtube.videos().list(
            part="snippet,statistics,liveStreamingDetails,localizations,topicDetails",
            id=video_ids
        )
        response = request.execute()
        video_items.extend(response["items"])

    return video_items


def is_youtube_shorts(videoId):
    shorts_url = f"https://www.youtube.com/shorts/{videoId}"
    response = requests.get(shorts_url, allow_redirects=True)

    if response.url == shorts_url:
        return True
    elif "watch?v=" in response.url:
        return False
    else:
        return None


def get_video_type(video_items):
    start_time = time.time()
    total = len(video_items)

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_video = {executor.submit(is_youtube_shorts, video["id"]): video for video in video_items}
        for i, future in enumerate(as_completed(future_to_video)):
            video = future_to_video[future]
            try:
                video["isShorts"] = future.result()
            except Exception as exc:
                print(f"{video['id']} generated an exception: {exc}")
            end_time = time.time()
            print(f"\rProgress: {i+1}/{total}   time: {int(end_time - start_time)} sec", end="")

    print(f"\r動画タイプを取得しました  time: {int(end_time - start_time)} sec")


def main(channelId, filename):
    uploads_playlist_id = get_uploads_playlist_id(channelId)
    video_id_list = get_video_id_in_playlist(uploads_playlist_id)
    print(f"動画IDを取得しました：{len(video_id_list)}本")
    video_items = get_video_items(video_id_list)
    print(f"動画データを取得しました：{len(video_items)}本")

    get_video_type(video_items)

    json_save(video_items, filename)



