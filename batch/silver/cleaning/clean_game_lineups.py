# clean_game_lineups.py

from pyspark.sql import functions as F
from pyspark.sql.types import *

TABLE = "game_lineups"

# ==================================================================================================================
#  Schema — all columns read as StringType first.
#  Reason: safe handling of any empty strings before casting.
# ==================================================================================================================

game_lineups_schema = StructType([
    StructField("game_lineups_id", StringType(), True),
    StructField("date",            StringType(), True),
    StructField("game_id",         StringType(), True),
    StructField("player_id",       StringType(), True),
    StructField("club_id",         StringType(), True),
    StructField("player_name",     StringType(), True),
    StructField("type",            StringType(), True),
    StructField("position",        StringType(), True),
    StructField("number",          StringType(), True),
    StructField("team_captain",    StringType(), True),
])


def clean_game_lineups(spark, bucket):

    S3_BRONZE_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
    S3_SILVER_PATH = f"s3a://{bucket}/silver/{TABLE}"

    print("\n Read Bronze:")

    df = spark.read.csv(
        S3_BRONZE_PATH,
        header=True,
        schema=game_lineups_schema
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
        .withColumn("game_id",      F.expr("try_cast(game_id as int)"))
        .withColumn("player_id",    F.expr("try_cast(player_id as int)"))
        .withColumn("club_id",      F.expr("try_cast(club_id as int)"))
        .withColumn("number",       F.expr("try_cast(number as int)"))
        .withColumn("team_captain", F.expr("try_cast(team_captain as int)"))
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Drop Invalid Jersey Numbers
    # — valid range: 1–99 (football rules)
    # — exploration found max = 880, clearly data error
    # — rows with null number (substitutes without a recorded number) are kept
    # ==================================================================================================================

    print("\n Drop Invalid Jersey Numbers")

    df = df.filter(
        F.col("number").isNull() |
        ((F.col("number") >= 1) & (F.col("number") <= 99))
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Standardize position Values
    # — exploration found inconsistent lowercase / abbreviated values:
    #     "midfield"  -> "Central Midfield"
    #     "Midfield"  -> "Central Midfield"
    #     "Attack"    -> "Forward"           (keep generic)
    # — proper values (Centre-Back, Goalkeeper, etc.) kept as-is
    # ==================================================================================================================

    print("\n Standardize position Values")

    df = df.withColumn(
        "position",
        F.when(F.lower(F.col("position")) == "midfield",  F.lit("Central Midfield"))
        .when(F.lower(F.col("position")) == "attack",    F.lit("Forward"))
        .otherwise(F.col("position"))
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Drop Rows with Null position
    # — only 3 nulls across 3.17M rows — drop rather than fill with "Unknown"
    # ==================================================================================================================

    print("\n Drop Rows with Null position")

    df = df.filter(F.col("position").isNotNull() & (F.trim(F.col("position")) != ""))

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Remove Duplicates
    # — exploration confirmed 0 duplicates on game_lineups_id, enforced defensively
    # ==================================================================================================================

    print("\n Remove Duplicates")

    df = df.dropDuplicates(["game_lineups_id"])

    print(f"{'-'*60}")


    # ==================================================================================================================
    # Save To S3
    # ==================================================================================================================

    print("\n================ Save Output to s3 as Parquet Files ================\n")

    df.write.mode("overwrite").parquet(S3_SILVER_PATH)

    print(f"\nSaved successfully to s3 bucket")

