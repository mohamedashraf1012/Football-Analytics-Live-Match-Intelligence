import os
import logging
import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

S3_BUCKET      = os.getenv("S3_BUCKET", "football-de-2026")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION     = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")

BRONZE_FILES = [
    "bronze/players.csv",
    "bronze/clubs.csv",
    "bronze/competitions.csv",
    "bronze/countries.csv",
    "bronze/games.csv",
    "bronze/appearances.csv",
    "bronze/game_events.csv",
    "bronze/game_lineups.csv",
    "bronze/player_valuations.csv",
    "bronze/transfers.csv",
    "bronze/club_games.csv",
    "bronze/national_teams.csv",
]


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id     = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY,
        region_name           = AWS_REGION,
    )


def check_bronze_file_exists(s3_key: str) -> bool:
    """Raises FileNotFoundError if the key doesn't exist in S3."""
    s3 = get_s3_client()
    try:
        size = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)["ContentLength"]
        log.info(f"[S3] found s3://{S3_BUCKET}/{s3_key} ({size / 1024:.1f} KB)")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            raise FileNotFoundError(
                f"[S3] missing s3://{S3_BUCKET}/{s3_key}"
            )
        raise


def check_all_bronze_files(**context) -> dict:
    """Check all 12 Bronze files exist. Fails the task if any are missing."""
    log.info(f"Checking {len(BRONZE_FILES)} Bronze files in s3://{S3_BUCKET}/")
    missing = []
    found   = []

    for key in BRONZE_FILES:
        try:
            check_bronze_file_exists(key)
            found.append(key)
        except FileNotFoundError as e:
            log.error(str(e))
            missing.append(key)

    if missing:
        raise RuntimeError(
            f"Bronze layer check failed — {len(missing)} file(s) missing:\n"
            + "\n".join(f"  - {f}" for f in missing)
        )

    log.info(f"Bronze layer OK — all {len(found)} files present.")
    return {"bucket": S3_BUCKET, "found": len(found), "missing": 0, "files": found}


def check_silver_parquet_exists(table_name: str) -> bool:
    """Check at least one Parquet file exists in silver/TABLE_NAME/."""
    s3       = get_s3_client()
    prefix   = f"silver/{table_name}/"
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    files    = [o for o in response.get("Contents", []) if o["Key"].endswith(".parquet")]

    if not files:
        raise FileNotFoundError(
            f"[S3] no Parquet files at s3://{S3_BUCKET}/{prefix} — "
            f"Spark job may have failed for {table_name}"
        )

    total_mb = sum(o["Size"] for o in files) / (1024 * 1024)
    log.info(f"[S3] found silver/{table_name}/ ({len(files)} file(s), {total_mb:.1f} MB)")
    return True


def verify_all_silver_outputs(**context) -> dict:
    """Check all 12 Silver Parquet outputs exist after the Spark job."""
    tables = [
        "players", "clubs", "competitions", "countries", "games",
        "appearances", "game_events", "game_lineups", "player_valuations",
        "transfers", "club_games", "national_teams",
    ]
    missing = []
    found   = []

    for table in tables:
        try:
            check_silver_parquet_exists(table)
            found.append(table)
        except FileNotFoundError as e:
            log.error(str(e))
            missing.append(table)

    if missing:
        raise RuntimeError(
            f"Silver verification failed — {len(missing)} table(s) missing Parquet output:\n"
            + "\n".join(f"  - {t}" for t in missing)
        )

    log.info(f"Silver verification OK — all {len(found)} tables have Parquet output.")
    return {"verified_tables": found, "count": len(found)}
