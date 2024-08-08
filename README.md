# YoutubeArchiver

## Description




## Environment
**ffmpegのダウンロードが必要です。**

- Python 3.11.3
- ffmpeg version N-111888-gfc993e7a53-20230902 Copyright (c) 2000-2023 the FFmpeg developers
- Docker version 26.1.1, build 4cf5afa

その他の必要ライブラリは[requirements.txt](/requirements.txt)に記載しています。



## Database
テスト環境ではDocker上でMySQLサーバーを動かしています

```PowerShell
$ cd .database

# ビルド
$ docker build -t mysql-local-server-image .
# 実行
$ docker run --name YoutubeArchiver-mysql-local -p 12345:3306 -v $PWD/temp:/var/lib/mysql -d mysql-local-server-image

# 停止
$ docker stop YoutubeArchiver-mysql-local
```



## License
Copyright (c) 2024 AmanoSpica.
This project is licensed under the MIT License, see the LICENSE.txt file for details.