import os
import boto3
import ignore.config as config

# الاتصال بـ AWS S3
s3 = boto3.client(
    's3',
    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
)

# الملفات اللي عايزين ننزلها
files_to_download = [
   'bronze/game_events.csv',
    'bronze/players.csv',
    'bronze/games.csv',
    'bronze/game_lineups.csv',
    'bronze/clubs.csv'
    'bronze/competitions.csv',
    'bronze/club_games.csv'
]

# مجلد التحميل المحلي
local_dir = 'datasets/raw_data'
os.makedirs(local_dir, exist_ok=True)

for s3_file_key in files_to_download:
    local_file_name = os.path.basename(s3_file_key)
    local_file_path = os.path.join(local_dir, local_file_name)

    print(f"Downloading {s3_file_key} ...")

    try:
        s3.download_file(config.S3_BUCKET, s3_file_key, local_file_path)
        file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
        print(f"✅ {local_file_name} — {file_size_mb:.2f} MB — saved to {os.path.abspath(local_file_path)}")

    except Exception as e:
        print(f"❌ Failed to download {s3_file_key}: {e}")

print("\nDone!")