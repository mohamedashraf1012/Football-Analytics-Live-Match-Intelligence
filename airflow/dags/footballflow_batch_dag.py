import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from utils.s3_helpers import check_all_bronze_files
from utils.snowflake_copy_sql import COPY_SQL
from utils.notifications import on_failure_callback, notify_pipeline_success

S3_BUCKET      = os.getenv("S3_BUCKET",         "football-de-2026")
PROJECT_DIR    = os.getenv("PROJECT_DIR",        "/opt/airflow/project")
DBT_PROJECT    = os.getenv("DBT_PROJECT_DIR",    "/opt/airflow/dbt")
DBT_PROFILES   = os.getenv("DBT_PROFILES_DIR",   "/opt/airflow/dbt")
SNOWFLAKE_CONN = "snowflake_footballflow"

# Join all 12 COPY INTO statements into one SQL block
ALL_COPY_SQL = "\n\n".join(COPY_SQL[t] for t in [
    "players", "clubs", "competitions", "countries", "games",
    "appearances", "game_events", "game_lineups",
    "player_valuations", "transfers", "club_games", "national_teams",
])

default_args = {
    "owner":               "footballflow",
    "depends_on_past":     False,
    "email_on_failure":    True,
    "email_on_retry":      False,
    "retries":             2,
    "retry_delay":         timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id            = "footballflow_batch_pipeline",
    description       = "FootballFlow batch pipeline: S3 → Spark → Snowflake → dbt",
    default_args      = default_args,
    schedule_interval = "0 2 * * *",   # daily at 02:00 UTC
    start_date        = datetime(2026, 6, 13),
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["footballflow", "batch", "silver", "gold"],
) as dag:

    # Wait for all 12 Bronze CSVs to land in S3 before doing anything
    check_bronze_files = S3KeySensor(
        task_id        = "check_bronze_files",
        bucket_name    = S3_BUCKET,
        bucket_key     = [
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
        ],
        wildcard_match = False,
        aws_conn_id    = "aws_footballflow",
        poke_interval  = 30,
        timeout        = 3600,
        mode           = "poke",
    )

    # Single Spark session cleans all 12 tables and writes Parquet to S3 silver/
    run_silver_cleaning = BashOperator(
        task_id      = "run_silver_cleaning",
        bash_command = f"""
            set -e
            cd {PROJECT_DIR}
            python run_all_silver.py
        """,
        execution_timeout = timedelta(hours=2),
    )

    # Confirm Spark actually wrote output before loading to Snowflake
    check_silver_files = S3KeySensor(
        task_id        = "check_silver_files",
        bucket_name    = S3_BUCKET,
        bucket_key     = [
            "silver/players/",
            "silver/clubs/",
            "silver/competitions/",
            "silver/countries/",
            "silver/games/",
            "silver/appearances/",
            "silver/game_events/",
            "silver/game_lineups/",
            "silver/player_valuations/",
            "silver/transfers/",
            "silver/club_games/",
            "silver/national_teams/",
        ],
        wildcard_match = True,
        aws_conn_id    = "aws_footballflow",
        poke_interval  = 20,
        timeout        = 600,
        mode           = "poke",
    )

    # COPY INTO all 12 RAW tables from S3 silver/ Parquet in one session
    load_to_snowflake = SnowflakeOperator(
        task_id           = "load_to_snowflake",
        snowflake_conn_id = SNOWFLAKE_CONN,
        sql               = ALL_COPY_SQL,
        warehouse         = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database          = os.getenv("SNOWFLAKE_DATABASE",  "FOOTBALLFLOW"),
        schema            = "RAW",
        role              = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        autocommit        = True,
    )

    # RAW → STAGING: rename columns, cast types, handle nulls
    dbt_run_staging = BashOperator(
        task_id      = "dbt_run_staging",
        bash_command = f"""
            set -e
            cd {DBT_PROJECT}
            dbt run \
                --profiles-dir {DBT_PROFILES} \
                --select staging \
                --no-version-check
        """,
        execution_timeout = timedelta(minutes=30),
    )

    # SCD Type 2 snapshot for player market values and club assignments
    dbt_snapshot_players = BashOperator(
        task_id      = "dbt_snapshot_players",
        bash_command = f"""
            set -e
            cd {DBT_PROJECT}
            dbt snapshot \
                --profiles-dir {DBT_PROFILES} \
                --select players_snapshot \
                --no-version-check
        """,
        execution_timeout = timedelta(minutes=20),
    )

    # Build Star Schema: dim_player, dim_club, dim_competition, dim_country, dim_date,
    # fact_appearances, fact_transfers, fact_game_events
    dbt_run_dims_facts = BashOperator(
        task_id      = "dbt_run_dims_facts",
        bash_command = f"""
            set -e
            cd {DBT_PROJECT}
            dbt run \
                --profiles-dir {DBT_PROFILES} \
                --select dims facts \
                --no-version-check
        """,
        execution_timeout = timedelta(minutes=30),
    )

    # Aggregated mart tables consumed by Power BI
    dbt_run_marts = BashOperator(
        task_id      = "dbt_run_marts",
        bash_command = f"""
            set -e
            cd {DBT_PROJECT}
            dbt run \
                --profiles-dir {DBT_PROFILES} \
                --select marts \
                --no-version-check
        """,
        execution_timeout = timedelta(minutes=30),
    )

    # Pipeline fails here if any not_null / unique / referential integrity test fails
    dbt_test = BashOperator(
        task_id      = "dbt_test",
        bash_command = f"""
            set -e
            cd {DBT_PROJECT}
            dbt test \
                --profiles-dir {DBT_PROFILES} \
                --no-version-check
        """,
        execution_timeout = timedelta(minutes=20),
    )

    (
        check_bronze_files
        >> run_silver_cleaning
        >> check_silver_files
        >> load_to_snowflake
        >> dbt_run_staging
        >> dbt_snapshot_players
        >> dbt_run_dims_facts
        >> dbt_run_marts
        >> dbt_test
    )
