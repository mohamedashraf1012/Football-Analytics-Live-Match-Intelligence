import os

S3_BUCKET  = os.getenv("S3_BUCKET", "football-de-2026")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")

STAGE       = "FOOTBALLFLOW.RAW.S3_SILVER_STAGE"
FILE_FORMAT = "FOOTBALLFLOW.RAW.PARQUET_FORMAT"


def copy_into_sql(table_name: str) -> str:
    return f"""
        COPY INTO FOOTBALLFLOW.RAW.{table_name.upper()}
        FROM @{STAGE}/silver/{table_name}/
        FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        PURGE = FALSE
        ON_ERROR = 'CONTINUE';
    """.strip()


COPY_SQL = {
    table: copy_into_sql(table)
    for table in [
        "players",
        "clubs",
        "competitions",
        "countries",
        "games",
        "appearances",
        "game_events",
        "game_lineups",
        "player_valuations",
        "transfers",
        "club_games",
        "national_teams",
    ]
}


# One-time Snowflake setup — run this manually in your Snowflake worksheet
SETUP_SQL = f"""
-- Create database and schemas
CREATE DATABASE IF NOT EXISTS FOOTBALLFLOW;
CREATE SCHEMA  IF NOT EXISTS FOOTBALLFLOW.RAW;
CREATE SCHEMA  IF NOT EXISTS FOOTBALLFLOW.STAGING;
CREATE SCHEMA  IF NOT EXISTS FOOTBALLFLOW.MARTS;

-- Parquet file format
CREATE OR REPLACE FILE FORMAT FOOTBALLFLOW.RAW.PARQUET_FORMAT
    TYPE = 'PARQUET'
    SNAPPY_COMPRESSION = TRUE;

-- S3 external stage — replace credentials with your actual AWS keys
CREATE OR REPLACE STAGE FOOTBALLFLOW.RAW.S3_SILVER_STAGE
    URL = 's3://{S3_BUCKET}/'
    CREDENTIALS = (
        AWS_KEY_ID     = '<YOUR_AWS_ACCESS_KEY_ID>'
        AWS_SECRET_KEY = '<YOUR_AWS_SECRET_ACCESS_KEY>'
    )
    FILE_FORMAT = (FORMAT_NAME = 'FOOTBALLFLOW.RAW.PARQUET_FORMAT');

-- Verify the stage can see your files
LIST @FOOTBALLFLOW.RAW.S3_SILVER_STAGE/silver/players/;

-- RAW tables (Parquet schema inferred via MATCH_BY_COLUMN_NAME)
CREATE TABLE IF NOT EXISTS FOOTBALLFLOW.RAW.PLAYERS           (v VARIANT);
CREATE TABLE IF NOT EXISTS FOOTBALLFLOW.RAW.CLUBS             (v VARIANT);
CREATE TABLE IF NOT EXISTS FOOTBALLFLOW.RAW.COMPETITIONS      (v VARIANT);
CREATE TABLE IF NOT EXISTS FOOTBALLFLOW.RAW.COUNTRIES         (v VARIANT);
CREATE TABLE IF NOT EXISTS FOOTBALLFLOW.RAW.GAMES             (v VARIANT);
CREATE TABLE IF NOT EXISTS FOOTBALLFLOW.RAW.APPEARANCES       (v VARIANT);
CREATE TABLE IF NOT EXISTS FOOTBALLFLOW.RAW.GAME_EVENTS       (v VARIANT);
CREATE TABLE IF NOT EXISTS FOOTBALLFLOW.RAW.GAME_LINEUPS      (v VARIANT);
CREATE TABLE IF NOT EXISTS FOOTBALLFLOW.RAW.PLAYER_VALUATIONS (v VARIANT);
CREATE TABLE IF NOT EXISTS FOOTBALLFLOW.RAW.TRANSFERS         (v VARIANT);
CREATE TABLE IF NOT EXISTS FOOTBALLFLOW.RAW.CLUB_GAMES        (v VARIANT);
CREATE TABLE IF NOT EXISTS FOOTBALLFLOW.RAW.NATIONAL_TEAMS    (v VARIANT);
"""
