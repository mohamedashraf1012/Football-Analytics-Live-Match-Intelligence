# clean_game_events.py

from pyspark.sql import functions as F
from pyspark.sql.types import *

TABLE = "game_events"

# ==================================================================================================================
#  Schema — all columns read as StringType first.
#  Reason: safe handling of any empty strings before casting.
# ==================================================================================================================

game_events_schema = StructType([
    StructField("game_event_id",    StringType(), True),
    StructField("date",             StringType(), True),
    StructField("game_id",          StringType(), True),
    StructField("minute",           StringType(), True),
    StructField("type",             StringType(), True),
    StructField("club_id",          StringType(), True),
    StructField("club_name",        StringType(), True),
    StructField("player_id",        StringType(), True),
    StructField("description",      StringType(), True),
    StructField("player_in_id",     StringType(), True),
    StructField("player_assist_id", StringType(), True),
])



def clean_game_events(spark, bucket):

    S3_BRONZE_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
    S3_SILVER_PATH = f"s3a://{bucket}/silver/{TABLE}"

    print("\n Read Bronze:")

    df = spark.read.csv(
        S3_BRONZE_PATH,
        header=True,
        schema=game_events_schema
    )

    # cleaning
    print(f"\ncleaning script for {TABLE} table started")

    # ==================================================================================================================
    # Trim All String Columns
    # ==================================================================================================================

    print("\n Trim All String Columns")

    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col_name, F.trim(F.col(col_name)))

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Drop Unused Columns
    # — player_in_id:     50.5% null — not needed 
    # — player_assist_id: 85.2% null — not needed 
    # ==================================================================================================================

    print("\n Drop Unused Columns")

    df = df.drop("player_in_id", "player_assist_id")

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Convert date String -> DateType
    # ==================================================================================================================

    print("\n Convert date -> Date")

    df = df.withColumn("date", F.to_date("date"))

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Safe Numeric Casting
    # ==================================================================================================================

    print("\n Safe Numeric Casting")

    df = (
        df
        .withColumn("game_id",   F.expr("try_cast(game_id as int)"))
        .withColumn("club_id",   F.expr("try_cast(club_id as int)"))
        .withColumn("player_id", F.expr("try_cast(player_id as int)"))
        .withColumn("minute",    F.expr("try_cast(minute as int)"))
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Normalize type
    # — Exploration confirmed exactly 4 values: Cards, Goals, Substitutions, Shootout
    # — Standardize to lowercase for consistency
    # ==================================================================================================================

    print("\n Normalize type")

    df = df.withColumn("type", F.lower(F.col("type")))

    print(f"{'-'*60}")


    # ==================================================================================================================
    # Fill String Nulls
    # — description: 7.3% null (93,180 rows) 
    #   event types; filled with "Unknown" 
    # ==================================================================================================================

    print("\n Fill String Nulls")

    df = df.fillna({"description": "Unknown"})

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Remove Duplicates
    # ==================================================================================================================

    print("\n Remove Duplicates")

    df = df.dropDuplicates(["game_event_id"])

    print(f"{'-'*60}")


    # ==================================================================================================================
    # Save To S3
    # ==================================================================================================================

    print("\n================ Save Output to s3 as Parquet Files ================\n")

    df.write.mode("overwrite").parquet(S3_SILVER_PATH)

    print(f"\nSaved successfully to s3 bucket")



   

