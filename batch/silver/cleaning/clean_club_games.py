# clean_club_games.py


from pyspark.sql import functions as F
from pyspark.sql.types import *

TABLE = "club_games"

# ==================================================================================================================
#  Schema — all columns read as StringType first.
#  Reason: safe handling of any empty strings before casting.
# ==================================================================================================================

club_games_schema = StructType([
    StructField("game_id",                 StringType(), True),
    StructField("club_id",                 StringType(), True),
    StructField("own_goals",               StringType(), True),
    StructField("own_position",            StringType(), True),
    StructField("own_manager_name",        StringType(), True),
    StructField("opponent_id",             StringType(), True),
    StructField("opponent_goals",          StringType(), True),
    StructField("opponent_position",       StringType(), True),
    StructField("opponent_manager_name",   StringType(), True),
    StructField("hosting",                 StringType(), True),
    StructField("is_win",                  StringType(), True),
])



def clean_club_games(spark, bucket):

    S3_BRONZE_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
    S3_SILVER_PATH = f"s3a://{bucket}/silver/{TABLE}"

    print("\n Read Bronze:")

    df = spark.read.csv(
        S3_BRONZE_PATH,
        header=True,
        schema=club_games_schema
    )

    # cleaning
    print(f"\ncleaning script for {TABLE} table started")
    # ==================================================================================================================
    # Drop Unnecessary Columns
    # — own_position / opponent_position:(non-league games have none),
    #   not needed for analysis
    # ==================================================================================================================
    
    print("\n Drop Unnecessary Columns")
    
    df = df.drop("own_position", "opponent_position")
    
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
    # Safe Numeric Casting
    # — own_position / opponent_position: null for non-league games (28.5%)
    #   kept as null — semantically correct, same rationale as games table
    # ==================================================================================================================

    print("\n Safe Numeric Casting")

    df = (
        df
        .withColumn("game_id",            F.expr("try_cast(game_id as int)"))
        .withColumn("club_id",            F.expr("try_cast(club_id as int)"))
        .withColumn("own_goals",          F.expr("try_cast(own_goals as int)"))
        .withColumn("opponent_id",        F.expr("try_cast(opponent_id as int)"))
        .withColumn("opponent_goals",     F.expr("try_cast(opponent_goals as int)"))
        .withColumn("is_win",             F.expr("try_cast(is_win as int)"))
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Derive Result Columns
    # — is_win = 1 already exists but encodes draws as 0 (same as losses)
    # — derive is_draw and is_loss so all three outcomes are explicit
    # — this makes mart_club_performance and mart_league_standings
    #   trivial GROUP BY aggregations in dbt with no CASE WHEN logic needed
    # ==================================================================================================================

    print("\n Derive Result Columns")

    df = (
        df
        .withColumn(
            "is_draw",
            (F.col("own_goals") == F.col("opponent_goals")).cast(IntegerType())
        )
        .withColumn(
            "is_loss",
            (F.col("own_goals") < F.col("opponent_goals")).cast(IntegerType())
        )
        # Re-derive is_win from goals for consistency (overwrite raw 0/1)
        .withColumn(
            "is_win",
            (F.col("own_goals") > F.col("opponent_goals")).cast(IntegerType())
        )
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Fill String Nulls
    # — manager names: 0.95% null — fill with "Unknown"
    # ==================================================================================================================

    print("\n Fill String Nulls")

    df = df.fillna({
        "own_manager_name":      "Unknown",
        "opponent_manager_name": "Unknown",
    })

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Remove Duplicates
    # — composite key: game_id + club_id
    # ==================================================================================================================

    print("\n Remove Duplicates")

    df = df.dropDuplicates(["game_id", "club_id"])

    print(f"{'-'*60}")



    # ==================================================================================================================
    # Save To S3
    # ==================================================================================================================

    print("\n================ Save Output to s3 as Parquet Files ================\n")

    df.write.mode("overwrite").parquet(S3_SILVER_PATH)

    print(f"\nSaved successfully to s3 bucket")


