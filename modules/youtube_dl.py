import yt_dlp
import urllib.request


def download_youtube_video(video_id, target_dir):
    ydl_opts = {
        'format': 'bestvideo+bestaudio/best',
        'outtmpl': f'{target_dir}/%(id)s.%(ext)s',
        'format_sort': ['vcodec:h264','res','acodec:m4a'],
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        url = f'https://www.youtube.com/watch?v={video_id}'
        ydl.download([url])
        video_file = f"{target_dir}/{video_id}.mp4"
        return video_file


def download_youtube_thumbnail(video_id, target_dir, url):
    file_name = f'{target_dir}/{video_id}.jpg'
    print(f"[urllib.request] Downloading thumbnail: {file_name}")
    urllib.request.urlretrieve(url, file_name)
    return file_name