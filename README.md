# YoutubeArchiver

## Description
Youtubeの指定したチャンネルのすべての動画を自分のYoutubeチャンネルにアップロード（アーカイブ）またはダウンロードするCLIアプリケーションです。


## GetStarted
```PowerShell
$ git clone git@github.com:AmanoSpica/YoutubeArchiver.git

# ffmpegがダウンロードされていない場合ここでダウンロード

$ python3 -m venv .venv

# (Powershell)
$ env\Scripts\Activate.ps1
# (Mac)
$ source env/bin/activate

$ pip install -r requirements.txt
$ python3 main.py
```
※環境によっては`python3`ではなく`python`の場合があります。



## Environment
**ffmpegのダウンロードが必要です。**

- Python 3.11.3
- ffmpeg version N-111888-gfc993e7a53-20230902 Copyright (c) 2000-2023 the FFmpeg developers

その他の必要ライブラリは[requirements.txt](/requirements.txt)に記載しています。

ffmpegのダウンロードに関しましては以下のサイトを参考にしてください。<br>
https://qiita.com/Tadataka_Takahashi/items/9dcb0cf308db6f5dc31b


## Environment Variables
### .env
- `YOUTUBE_API_KEY`
  - Youtube Data API v3が有効になっているAPIキー
- `TARGET_YOUTUBE_CHANNEL_ID`
  - ダウンロードまたはアーカイブするYoutubeチャンネルID
- `UPLOAD_YOUTUBE_CHANNEL_ID`
  - アップロードするYoutubeチャンネルID

**【データベース接続情報】**
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_DATABASE`


### client_secret.json
Youtube Data API v3が有効になっているOAuthクライアントキー

identityディレクトリに保存してください。アップローダーは`client_secret_YoutubeArchiver-uploader<NUMBER>.json`というファイル名で保存してください。


## How to Use
プログラムを実行すると始めにGoogle OAuthの同意画面が表示されます。"メインAPIクライアント"と"アップローダー1"の2画面表示されるのでユーザーを選んで続行してください。

その後、YoutubeからTargetChannelのすべての動画のデータを取得、データベースに保存または更新します。

ダウンロードアップロード処理に進みます。CLIの通り進めてください。<br>
アップローダーは動画6本ごとに自動的に変更されます。その際にGoogle OAuthの同意画面が表示されるのでユーザーを選んで続行してください。



## Database
テスト環境ではDocker上でMySQLサーバーを動かしています。本番環境はGCPのSQLに接続してください。

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