import os
import json
import time
import datetime
import asyncio
import threading

import schedule
import pandas as pd
import requests
from dotenv import load_dotenv, find_dotenv
from term_printer import Color, cprint

from modules.db import DBManager
from modules.youtube_dl import download_youtube_video, download_youtube_thumbnail
from modules.youtube_uploader import YoutubeVideoManager


load_dotenv(find_dotenv())

db = DBManager(
    db_user=os.getenv("DB_USER"),
    db_password=os.getenv("DB_PASSWORD"),
    db_host=os.getenv("DB_HOST"),
    db_port=os.getenv("DB_PORT"),
    db_database=os.getenv("DB_DATABASE")
)

youtube = YoutubeVideoManager(
    api_key=os.getenv("YOUTUBE_API_KEY"),
    target_channel_id=os.getenv("TARGET_YOUTUBE_CHANNEL_ID"),
    upload_channel_id=os.getenv("UPLOAD_YOUTUBE_CHANNEL_ID"),
    max_threads=20
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



def download_video(video: pd.DataFrame, startTime: float):
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
    response = youtube.upload_video(
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
            f"UPDATE TargetVideo SET isPushed = 1, uploadVideoId = '{response['id']}' WHERE id = '{video['id']}';"
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

    # 前回の処理でアップロードされていない動画があるか確認
    remain_videos = asyncio.run(
        db.query(
            "SELECT id FROM TargetVideo WHERE isDownloaded = 1 AND isPushed = 0 ORDER BY publishedAt ASC;"
        )
    )

    if not remain_videos.empty:
        cprint(f"前回の処理でアップロードされていない動画が{len(remain_videos)}本あります。", attrs=[Color.BRIGHT_RED])
        cprint("アップロードを続行しますか？ (y/n): ", attrs=[Color.CYAN], end="")
        is_upload_remain = input()
        if is_upload_remain == "y" or is_upload_remain == "Y" or is_upload_remain == "yes" or is_upload_remain == "Yes":
            startTime = time.time()
            for i in range(len(remain_videos)):
                video = asyncio.run(db.query(f"SELECT * FROM TargetVideo WHERE isDownloaded = 1 AND isPushed = 0 ORDER BY publishedAt ASC LIMIT 1;")).iloc[0]
                if video.empty:
                    continue
                cprint(f"\nProgress:  ({i + 1}/{len(remain_videos)}) {video['title']}", attrs=[Color.BRIGHT_YELLOW])
                post_webhook(f"\nProgress:  ({i + 1}/{len(remain_videos)}) {video['title']}")

                post_webhook(f"[Upload] Start: {video['title']}  ({video['id']})")
                progress_time = time.time()
                video_file_path = f"temp/videos/{video['id']}.mp4"
                thumbnail_file_path = f"temp/thumbnails/{video['id']}.jpg"
                upload_video(video, video_file_path, thumbnail_file_path, startTime)
                post_webhook(f"[Upload] Complete: {video['title']}  ({video['id']})")

                delete_temp_files(video, video_file_path, thumbnail_file_path)

                cprint(f"Finished: {video['title']}", attrs=[Color.GREEN])
                post_webhook(f"Finished: {video['title']}  ({video['id']})\nTotal Time: {time.time() - startTime:.2f} sec")
                print(f"Time: {time.time() - progress_time:.2f} sec")
                print(f"Total Time [remained_videos]: {time.time() - startTime:.2f} sec\n")

            post_webhook(f"前回の処理でアップロードされていなかった動画のアップロードが完了しました。\nTotal Time: {time.time() - startTime:.2f} sec")
            cprint("\n前回の処理でアップロードされていなかった動画のアップロードが完了しました。", attrs=[Color.MAGENTA])
            print(f"Total Time [remained_videos]: {time.time() - startTime:.2f} sec\n")

    cprint("\n何本の動画をダウンロードしますか？ (投稿日時が古いものから処理します): ", attrs=[Color.CYAN], end="")
    output_video_number = input()
    if not output_video_number.isdecimal():
        cprint("数字を入力してください。", attrs=[Color.RED])
        return

    output_video_number = int(output_video_number)
    cprint("ダウンロードした動画をアップロードしますか？ (y/n): ", attrs=[Color.CYAN], end="")
    is_upload = input()

    if is_upload == "y" or is_upload == "Y" or is_upload == "yes" or is_upload == "Yes":
        is_upload = True
        cprint("\nダウンロード・アップロードを開始します。", attrs=[Color.MAGENTA])


    else:
        is_upload = False
        cprint("\nダウンロードを開始します。", attrs=[Color.BRIGHT_YELLOW])

    print(f"\nGetting {output_video_number} videos...")
    startTime = time.time()
    for i in range(output_video_number):
        video = asyncio.run(db.query(f"SELECT * FROM TargetVideo WHERE isDownloaded = 0 AND isPushed = 0 ORDER BY publishedAt ASC LIMIT 1;")).iloc[0]
        if video.empty:
            continue
        cprint(f"\nProgress:  ({i + 1}/{output_video_number}) {video['title']}", attrs=[Color.BRIGHT_YELLOW])
        post_webhook(f"\nProgress:  ({i + 1}/{output_video_number}) {video['title']}")

        post_webhook(f"[Download] Start: {video['title']}  ({video['id']})")
        progress_time = time.time()
        video_file_path, thumbnail_file_path = download_video(video, startTime)
        post_webhook(f"[Download] Complete: {video['title']}  ({video['id']})")

        if is_upload:
            post_webhook(f"[Upload] Start: {video['title']}  ({video['id']})")
            upload_video(video, video_file_path, thumbnail_file_path, startTime)
            post_webhook(f"[Upload] Complete: {video['title']}  ({video['id']})")

            delete_temp_files(video, video_file_path, thumbnail_file_path)

        cprint(f"Finished: {video['title']}", attrs=[Color.GREEN])
        post_webhook(f"Finished: {video['title']}  ({video['id']})\nTotal Time: {time.time() - startTime:.2f} sec")
        print(f"Time: {time.time() - progress_time:.2f} sec")
        print(f"Total Time: {time.time() - startTime:.2f} sec\n")


    cprint("\nダウンロード・アップロードが完了しました。", attrs=[Color.MAGENTA])
    print(f"Total Time: {time.time() - startTime:.2f} sec\n")
    post_webhook(f"すべての動画のダウンロード・アップロードが完了しました。\nTotal Time: {time.time() - startTime:.2f} sec")


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


def post_webhook(description):
    webhook_url = "https://discord.com/api/webhooks/1272407304284667925/adUs5sYJ36kGm7t_Zq_5iHNK0rDJWMG1UoORPZWj5JdToZFc82CgzDWr2PpmS3Q5pbng"
    payload = {"content": description}
    try:
        response = requests.post(webhook_url, json=payload, timeout=5)
        response.raise_for_status()
        response.close()

    except TimeoutError:
        print(f"Failed to send log to Discord: Timeout")

    except Exception as e:
        print(f"Failed to send log to Discord: {e}")


def update_quota():
    query = "UPDATE QuotaData SET quota = 0;"
    asyncio.run(db.query(query))


def scheduler():
    while True:
        schedule.run_pending()
        time.sleep(5)



def main():
    schedule.every().day.at("16:00").do(update_quota)
    schedule.every().day.at("00:00").do(youtube.save_video_data)
    schedule_thread = threading.Thread(target=scheduler)
    schedule_thread.start()

    youtube.save_video_data()
    CLI_dl_and_up()




if __name__ == "__main__":
    cprint("##### Start #####", attrs=[Color.BRIGHT_RED])
    main()
    cprint("##### End #####", attrs=[Color.BRIGHT_RED])
