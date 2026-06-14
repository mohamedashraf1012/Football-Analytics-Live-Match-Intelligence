# clean_appearances.py

from pyspark.sql import functions as F
from pyspark.sql.types import *

TABLE = "appearances"

# ==================================================================================================================
#  Schema — all columns read as StringType first.
#  Reason: safe handling of any empty strings before casting.
# ==================================================================================================================

appearances_schema = StructType([
    StructField("appearance_id",          StringType(), True),
    StructField("game_id",                StringType(), True),
    StructField("player_id",              StringType(), True),
    StructField("player_club_id",         StringType(), True),
    StructField("player_current_club_id", StringType(), True),
    StructField("date",                   StringType(), True),
    StructField("player_name",            StringType(), True),
    StructField("competition_id",         StringType(), True),
    StructField("yellow_cards",           StringType(), True),
    StructField("red_cards",              StringType(), True),
    StructField("goals",                  StringType(), True),
    StructField("assists",                StringType(), True),
    StructField("minutes_played",         StringType(), True),
])



def clean_appearances(spark, bucket):

    S3_BRONZE_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
    S3_SILVER_PATH = f"s3a://{bucket}/silver/{TABLE}"

    print("\n Read Bronze:")

    df = spark.read.csv(
        S3_BRONZE_PATH,
        header=True,
        schema=appearances_schema
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
        .withColumn("game_id",                F.expr("try_cast(game_id as int)"))
        .withColumn("player_id",              F.expr("try_cast(player_id as int)"))
        .withColumn("player_club_id",         F.expr("try_cast(player_club_id as int)"))
        .withColumn("player_current_club_id", F.expr("try_cast(player_current_club_id as int)"))
        .withColumn("yellow_cards",           F.expr("try_cast(yellow_cards as int)"))
        .withColumn("red_cards",              F.expr("try_cast(red_cards as int)"))
        .withColumn("goals",                  F.expr("try_cast(goals as int)"))
        .withColumn("assists",                F.expr("try_cast(assists as int)"))
        .withColumn("minutes_played",         F.expr("try_cast(minutes_played as int)"))
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Handle Invalid minutes_played

    # ==================================================================================================================

    print("\nHandle Invalid minutes_played")

    df = df.filter(
        (F.col("minutes_played") >= 1) &
        (F.col("minutes_played") <= 135)
    )

    print(f"{'-'*60}")


    # ==================================================================================================================
    # Fill String Nulls
    # — player_name: only 2 nulls across 1.88M rows
    # ==================================================================================================================

    print("\n Fill String Nulls")

    df = df.fillna({"player_name": "Unknown"})

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Remove Duplicates
    # — exploration confirmed 0 duplicates on appearance_id, enforced defensively
    # ==================================================================================================================

    print("\n Remove Duplicates")

    df = df.dropDuplicates(["appearance_id"])

    print(f"{'-'*60}")



    # ==================================================================================================================
    # Save To S3
    # ==================================================================================================================

    print("\n================ Save Output to s3 as Parquet Files ================\n")

    df.write.mode("overwrite").parquet(S3_SILVER_PATH)

    print(f"\nSaved successfully to s3 bucket")




