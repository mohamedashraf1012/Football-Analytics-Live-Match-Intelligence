import boto3
import sys
import config

# ■■ Config ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
BUCKET = config.S3_BUCKET
PREFIX = config.S3_BRONZE_PREFIX
EXPECTED_FILES = [
'players.csv', 'clubs.csv', 'competitions.csv',
'games.csv', 'appearances.csv', 'game_events.csv',
'game_lineups.csv', 'player_valuations.csv', 'transfers.csv',
'club_games.csv', 'national_teams.csv',
'countries.csv',
]

# ■■ Connect to S3 ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
s3 = boto3.client(
    's3',
    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
    region_name=config.AWS_REGION
)

# ■■ List files in bronze/ ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
found = {}
for obj in response.get('Contents', []):
    key = obj['Key'].replace(PREFIX, '')
    if key:
        size = obj['Size'] / (1024 * 1024)  # convert to MB
        found[key] = size

# ■■ Check each expected file ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
print('=' * 55)
print(' BRONZE LAYER VERIFICATION REPORT')
print('=' * 55)
all_ok = True
for f in EXPECTED_FILES:
    if f in found:
        print(f' OK {f:<40} {found[f]:>6.1f} MB')
    else:
        print(f' MISSING {f}')
        all_ok = False
print('=' * 55)
if all_ok:
    print(' ALL 12 FILES PRESENT. Bronze layer ready.')
    print(f' Bucket: s3://{BUCKET}/{PREFIX}')
else:
    print(' WARNING: Some files are missing. Re-upload before handing off.')
    sys.exit(1)
