import os
import json
import time

import modules.get_video_data as yt_gvd


channel_id = "UC1opHUrw8rvnsadT-iGp7Cg"
video_data_file = "data/videos.json"



if __name__ == "__main__":
    print("##### Start #####")
    yt_gvd.main(channel_id, video_data_file)
    print("##### End #####")