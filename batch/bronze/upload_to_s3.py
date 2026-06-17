"""
upload_to_s3.py
---------------
Uploads the 12 raw CSV files as-is to the S3 bronze/ prefix.
No transformation — bronze = exact copy of source files.

Bucket  : football-de-2026
Prefix  : bronze/
"""

import os
import boto3
from botocore.exceptions import ClientError

# ── Config ────────────────────────────────────────────────────────────────────
BUCKET    = "football-de-2026"
PREFIX    = "bronze/"
DATA_DIR  = "./data"          # local folder with the CSVs

CSV_FILES = [
    "players.csv",
    "clubs.csv",
    "competitions.csv",
    "transfers.csv",
    "matches.csv",
    "game_events.csv",
    "appearances.csv",
    "club_games.csv",
    "player_valuations.csv",
    "games.csv",
    "game_lineups.csv",
    "game_stats.csv",
]
# ─────────────────────────────────────────────────────────────────────────────


def upload_file(s3_client, local_path: str, s3_key: str) -> bool:
    try:
        s3_client.upload_file(local_path, BUCKET, s3_key)
        size_mb = os.path.getsize(local_path) / (1024 ** 2)
        print(f"  ✅  {s3_key:<45}  ({size_mb:.2f} MB)")
        return True
    except ClientError as exc:
        print(f"  ❌  {s3_key}  →  {exc}")
        return False


def main() -> None:
    print(f"\n🚀  Uploading raw CSVs  →  s3://{BUCKET}/{PREFIX}")
    print(f"    Local source : {os.path.abspath(DATA_DIR)}\n")

    s3 = boto3.client("s3")
    ok, fail = 0, 0

    for fname in CSV_FILES:
        local_path = os.path.join(DATA_DIR, fname)
        s3_key     = f"{PREFIX}{fname}"

        if not os.path.isfile(local_path):
            print(f"  ⚠️   Skipped (not found) : {fname}")
            fail += 1
            continue

        success = upload_file(s3, local_path, s3_key)
        if success:
            ok += 1
        else:
            fail += 1

    print(f"\n{'─'*55}")
    print(f"  Uploaded : {ok}  |  Failed/Skipped : {fail}")
    print(f"  Bucket   : s3://{BUCKET}/{PREFIX}")
    print(f"{'─'*55}\n")


if __name__ == "__main__":
    main()
