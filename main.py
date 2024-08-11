import os
import json
import time
import datetime
import asyncio

import pandas as pd
from dotenv import load_dotenv, find_dotenv
from term_printer import Color, cprint, StdText

from modules.db import DBManager
from modules.get_video_data import GetVideoData
from modules.youtube_dl import download_youtube_video, download_youtube_thumbnail
from modules.youtube_uploader import YoutubeVideoManager


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

identity_files = []
for i in range(1, 11):
    file_name = f"identity/client_secret_YoutubeArchiver-uploader{str(i).zfill(2)}.json"
    if os.path.exists(file_name):
        identity_files.append(file_name)

youtube = YoutubeVideoManager(
    upload_channel_id=os.getenv("UPLOAD_YOUTUBE_CHANNEL_ID"),
    identity_file="identity/client_secret.json",
    uploader_identity_file=identity_files
)


def load_json(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


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


def download_video(video: pd.DataFrame, startTime: float, number: int, totalVideos: int):
    cprint(f"\nProgress:  ({number + 1}/{totalVideos}) {video['title']}", attrs=[Color.BRIGHT_YELLOW])

    progress_time = time.time()
    video_file_path = download_youtube_video(video["id"], "temp/videos")
    cprint(f"Downloaded: {video_file_path}", attrs=[Color.BRIGHT_GREEN])
    print(f"Time: {time.time() - progress_time:.2f} sec")
    print(f"Total Time: {time.time() - startTime:.2f} sec\n")

    progress_time = time.time()
    thumbnail_file_path = download_youtube_thumbnail(video["id"], "temp/thumbnails", video["thumbnails_url"])
    cprint(f"Downloaded: {thumbnail_file_path} (thumbnail)", attrs=[Color.BRIGHT_GREEN])
    print(f"Time: {time.time() - progress_time:.2f} sec")
    print(f"Total Time: {time.time() - startTime:.2f} sec\n")

    # Update database
    asyncio.run(
        db.query(
            f"UPDATE TargetVideo SET isDownloaded = 1 WHERE id = '{video['id']}';"
        )
    )
    return video_file_path, thumbnail_file_path


def upload_video(video: pd.DataFrame, video_file_path: str, thumbnail_file_path: str, startTime: float):
    progress_time = time.time()
    cprint(f"Upload Progress: {video['title']}", attrs=[Color.BRIGHT_YELLOW])

    title, description = format_video_info(video)
    youtube.upload_video(
        video_file_path=video_file_path,
        title=title,
        description=description,
        category_id=video["categoryId"],
        thumbnail_file_path=thumbnail_file_path,
        tags=video["tags"]
    )

    cprint(f"Uploaded: {video['title']}", attrs=[Color.BRIGHT_GREEN])
    print(f"Time: {time.time() - progress_time:.2f} sec")
    print(f"Total Time: {time.time() - startTime:.2f} sec\n")

    # Update database
    asyncio.run(
        db.query(
            f"UPDATE TargetVideo SET isPushed = 1 WHERE id = '{video['id']}';"
        )
    )


def delete_temp_files(video: pd.DataFrame, video_file_path: str, thumbnail_file_path: str):
    temp_files = [video_file_path, thumbnail_file_path]
    for temp_file in temp_files:
        os.remove(temp_file)
        print(f"Removed: {temp_file}")
        # Update database
        asyncio.run(
            db.query(
                f"UPDATE TargetVideo SET isDownloaded = 0 WHERE id = '{video['id']}';"
            )
        )



def CLI_dl_and_up():
    cprint("Youtube動画データを取得・更新しました。", attrs=[Color.BRIGHT_GREEN])
    cprint("ダウンロードを開始するにはEnterキーを押してください。", attrs=[Color.BRIGHT_GREEN], end="")
    input()

    cprint("\n何本の動画をダウンロードしますか？ (投稿日時が古いものから処理します): ", attrs=[Color.CYAN], end="")
    output_video_number = input()
    if not output_video_number.isdecimal():
        cprint("数字を入力してください。", attrs=[Color.RED])
        return

    output_video_number = int(output_video_number)
    if not int(output_video_number) <= 60:
        cprint("60本以上の動画をダウンロードすることはできません。最大の60本を処理します。", attrs=[Color.RED])
        output_video_number = 60
    cprint("ダウンロードした動画をアップロードしますか？ (y/n): ", attrs=[Color.CYAN], end="")
    is_upload = input()

    if is_upload == "y" or is_upload == "Y" or is_upload == "yes" or is_upload == "Yes":
        is_upload = True
        cprint("\nダウンロード・アップロードを開始します。", attrs=[Color.MAGENTA])

        remain_videos = asyncio.run(
            db.query(
                "SELECT * FROM TargetVideo WHERE isDownloaded = 1 AND isPushed = 0 ORDER BY publishedAt ASC;"
            )
        )

        if not remain_videos.empty:
            cprint(f"前回の処理でアップロードされていない動画が{len(remain_videos)}本あります。", attrs=[Color.BRIGHT_RED])
            cprint("アップロードを続行しますか？ (y/n): ", attrs=[Color.CYAN], end="")
            is_upload_remain = input()
            if is_upload_remain == "y" or is_upload_remain == "Y" or is_upload_remain == "yes" or is_upload_remain == "Yes":
                startTime = time.time()
                for i, video in remain_videos.iterrows():
                    progress_time = time.time()
                    video_file_path = f"temp/videos/{video['id']}.mp4"
                    thumbnail_file_path = f"temp/thumbnails/{video['id']}.jpg"
                    upload_video(video, video_file_path, thumbnail_file_path, startTime)
                    delete_temp_files(video, video_file_path, thumbnail_file_path)

                    cprint(f"Finished: {video['title']}", attrs=[Color.GREEN])
                    print(f"Time: {time.time() - progress_time:.2f} sec")
                    print(f"Total Time [remained_videos]: {time.time() - startTime:.2f} sec\n")

                cprint("\n前回の処理でアップロードされていなかった動画のアップロードが完了しました。", attrs=[Color.MAGENTA])
                print(f"Total Time [remained_videos]: {time.time() - startTime:.2f} sec\n")

    else:
        is_upload = False
        cprint("\nダウンロードを開始します。", attrs=[Color.BRIGHT_YELLOW])

    target_videos: pd.DataFrame = asyncio.run(
        db.query(
            f"SELECT * FROM TargetVideo WHERE isDownloaded = 0 AND isPushed = 0 ORDER BY publishedAt ASC LIMIT {output_video_number};"
        )
    )

    print(f"\nGetting {len(target_videos)} videos...")
    startTime = time.time()
    for i, video in target_videos.iterrows():
        progress_time = time.time()
        video_file_path, thumbnail_file_path = download_video(video, startTime, i, len(target_videos))

        if is_upload:
            upload_video(video, video_file_path, thumbnail_file_path, startTime)
            delete_temp_files(video, video_file_path, thumbnail_file_path)

        cprint(f"Finished: {video['title']}", attrs=[Color.GREEN])
        print(f"Time: {time.time() - progress_time:.2f} sec")
        print(f"Total Time: {time.time() - startTime:.2f} sec\n")


    cprint("\nダウンロード・アップロードが完了しました。", attrs=[Color.MAGENTA])
    print(f"Total Time: {time.time() - startTime:.2f} sec\n")



def main():
    # get_video_data.save_video_data()
    video_data = load_json("data/videos.json")
    asyncio.run(save_to_database(video_data))
    CLI_dl_and_up()



if __name__ == "__main__":
    cprint("##### Start #####", attrs=[Color.BRIGHT_RED])
    main()
    cprint("##### End #####", attrs=[Color.BRIGHT_RED])
