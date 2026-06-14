# clean_games.py


from pyspark.sql import functions as F
from pyspark.sql.types import *

TABLE = "games"

# ==================================================================================================================
#  Schema — all columns read as StringType first.
#  Reason: safe handling of any empty strings before casting.
# ==================================================================================================================
games_schema = StructType([
    StructField("game_id",                  StringType(), True),
    StructField("competition_id",           StringType(), True),
    StructField("season",                   StringType(), True),
    StructField("round",                    StringType(), True),
    StructField("date",                     StringType(), True),
    StructField("home_club_id",             StringType(), True),
    StructField("away_club_id",             StringType(), True),
    StructField("home_club_goals",          StringType(), True),
    StructField("away_club_goals",          StringType(), True),
    StructField("home_club_position",       StringType(), True),
    StructField("away_club_position",       StringType(), True),
    StructField("home_club_manager_name",   StringType(), True),
    StructField("away_club_manager_name",   StringType(), True),
    StructField("stadium",                  StringType(), True),
    StructField("attendance",               StringType(), True),
    StructField("referee",                  StringType(), True),
    StructField("url",                      StringType(), True),
    StructField("home_club_formation",      StringType(), True),
    StructField("away_club_formation",      StringType(), True),
    StructField("home_club_name",           StringType(), True),
    StructField("away_club_name",           StringType(), True),
    StructField("aggregate",                StringType(), True),
    StructField("competition_type",         StringType(), True),
])



def clean_games(spark, bucket):

    S3_BRONZE_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
    S3_SILVER_PATH = f"s3a://{bucket}/silver/{TABLE}"

    print("\n Read Bronze:")

    df = spark.read.csv(
        S3_BRONZE_PATH,
        header=True,
        schema=games_schema
    )

    # cleaning
    print(f"\ncleaning script for {TABLE} table started")

    # ==================================================================================================================
    # Drop Unnecessary Columns
    # ==================================================================================================================

    print("\n Drop Unnecessary Columns")

    COLS_TO_DROP = [
        "url",
        "stadium",
        "attendance",
        "referee",
    ]

    df = df.drop(*COLS_TO_DROP)

    print(f"{'-'*60}")

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
    # — home_club_position / away_club_position: null for all non-league games (28.5%)
    #   kept as null — a 0 would be meaningless for cup/international games
    # ==================================================================================================================

    print("\n Safe Numeric Casting")

    df = (
        df
        .withColumn("game_id",            F.expr("try_cast(game_id as int)"))
        .withColumn("season",             F.expr("try_cast(season as int)"))
        .withColumn("home_club_id",       F.expr("try_cast(home_club_id as int)"))
        .withColumn("away_club_id",       F.expr("try_cast(away_club_id as int)"))
        .withColumn("home_club_goals",    F.expr("try_cast(home_club_goals as int)"))
        .withColumn("away_club_goals",    F.expr("try_cast(away_club_goals as int)"))
        .withColumn("home_club_position", F.expr("try_cast(home_club_position as int)"))
        .withColumn("away_club_position", F.expr("try_cast(away_club_position as int)"))
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Fill String Nulls
    # — manager names: small null rates (<1%)          — fill with "Unknown"
    # — formations: 9% null including non-league games — fill with "Unknown"
    # — club names : tiny null rates (<0.1%)           — fill with "Unknown"
    # — home/away position intentionally left as null
    #   (null is semantically correct for non-league games)
    # ==================================================================================================================

    print("\n Fill String Nulls")

    df = df.fillna({
        "home_club_manager_name": "Unknown",
        "away_club_manager_name": "Unknown",
        "home_club_formation":    "Unknown",
        "away_club_formation":    "Unknown",
        "home_club_name":         "Unknown",
        "away_club_name":         "Unknown",
    })

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Remove Duplicates
    # — exploration confirmed 0 duplicates on game_id, enforced defensively
    # ==================================================================================================================

    print("\n Remove Duplicates")

    df = df.dropDuplicates(["game_id"])

    print(f"{'-'*60}")


    # ==================================================================================================================
    # Save To S3
    # ==================================================================================================================

    print("\n================ Save Output to s3 as Parquet Files ================\n")

    df.write.mode("overwrite").parquet(S3_SILVER_PATH)

    print(f"\nSaved successfully to s3 bucket")


