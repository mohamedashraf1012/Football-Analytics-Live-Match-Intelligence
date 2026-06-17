"""
verify_bronze.py
----------------
Verifies that all 12 CSV files are present in the S3 bronze/ prefix
and prints their size & last-modified date.

Bucket : football-de-2026
Prefix : bronze/
"""

import boto3
from datetime import timezone
from botocore.exceptions import ClientError

# ── Config ────────────────────────────────────────────────────────────────────
BUCKET   = "football-de-2026"
PREFIX   = "bronze/"

EXPECTED = {
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
}
# ─────────────────────────────────────────────────────────────────────────────


def list_bronze(s3_client) -> list[dict]:
    """Return all objects under bronze/ prefix."""
    paginator = s3_client.get_paginator("list_objects_v2")
    objects   = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            objects.append(obj)
    return objects


def main() -> None:
    print(f"\n🔎  Verifying Bronze Layer — s3://{BUCKET}/{PREFIX}\n")

    s3 = boto3.client("s3")

    try:
        objects = list_bronze(s3)
    except ClientError as exc:
        print(f"  ❌  Could not list bucket: {exc}")
        return

    if not objects:
        print("  ❌  No files found in bronze/ prefix.")
        return

    found_names = set()
    total_size  = 0

    print(f"  {'FILE':<40} {'SIZE (MB)':>10}  LAST MODIFIED")
    print(f"  {'─'*40}  {'─'*10}  {'─'*22}")

    for obj in sorted(objects, key=lambda x: x["Key"]):
        fname    = obj["Key"].replace(PREFIX, "")
        size_mb  = obj["Size"] / (1024 ** 2)
        modified = obj["LastModified"].astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        print(f"  {fname:<40} {size_mb:>10.2f}  {modified}")
        found_names.add(fname)
        total_size += obj["Size"]

    missing = EXPECTED - found_names
    extra   = found_names - EXPECTED

    print(f"\n  {'─'*60}")
    print(f"  Total files : {len(found_names)}  |  Total size : {total_size / (1024**2):.2f} MB")

    if missing:
        print(f"\n  ⚠️   Missing files ({len(missing)}):")
        for f in sorted(missing):
            print(f"       - {f}")
    else:
        print(f"\n  ✅  All {len(EXPECTED)} expected files are present in bronze/")

    if extra:
        print(f"\n  ℹ️   Extra files not in expected list:")
        for f in sorted(extra):
            print(f"       + {f}")

    print()


if __name__ == "__main__":
    main()
