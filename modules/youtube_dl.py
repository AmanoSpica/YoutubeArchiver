import yt_dlp

def download_youtube_video(video_id):
    ydl_opts = {
        'format': 'bestvideo+bestaudio/best',
        'outtmpl': '%(id)s.%(ext)s',
        'merge_output_format': 'mp4',
        'postprocessors': [{
            'key': 'FFmpegVideoConvertor',
            'preferedformat': 'mp4',
        }]
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        url = f'https://www.youtube.com/watch?v={video_id}'
        info_dict = ydl.extract_info(url, download=True)
        title = info_dict.get('title', None)
        video_file = title + ".mp4"
        return video_file