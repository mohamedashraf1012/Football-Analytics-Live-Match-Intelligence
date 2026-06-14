# clean_players.py

from pyspark.sql import functions as F
from pyspark.sql.types import *

TABLE = "players"

# ==================================================================================================================
#  Schema — all columns read as StringType first.
#  Reason: safe handling of any empty strings before casting.
# ==================================================================================================================

players_schema = StructType([
    StructField("player_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("name", StringType(), True),
    StructField("last_season", IntegerType(), True),
    StructField("current_club_id", IntegerType(), True),
    StructField("player_code", StringType(), True),
    StructField("country_of_birth", StringType(), True),
    StructField("city_of_birth", StringType(), True),
    StructField("country_of_citizenship", StringType(), True),
    StructField("date_of_birth", TimestampType(), True),
    StructField("sub_position", StringType(), True),
    StructField("position", StringType(), True),
    StructField("foot", StringType(), True),
    StructField("height_in_cm", IntegerType(), True),
    StructField("contract_expiration_date", TimestampType(), True),
    StructField("agent_name", StringType(), True),
    StructField("image_url", StringType(), True),
    StructField("international_caps", IntegerType(), True),
    StructField("international_goals", IntegerType(), True),
    StructField("current_national_team_id", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("current_club_domestic_competition_id", StringType(), True),
    StructField("current_club_name", StringType(), True),
    StructField("market_value_in_eur", IntegerType(), True),
    StructField("highest_market_value_in_eur", IntegerType(), True)
])




def clean_players(spark, bucket):

    S3_BRONZE_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
    S3_SILVER_PATH = f"s3a://{bucket}/silver/{TABLE}"

    print("\n Read Bronze:")

    df = spark.read.csv(
        S3_BRONZE_PATH,
        header=True,
        schema=players_schema
    )

    # cleaning
    print(f"\ncleaning script for {TABLE} table started")

    # ==================================================================================================================
    # Drop Unnecessary Columns
    # ==================================================================================================================

    print("\n Drop Unnecessary Columns")

    COLS_TO_DROP = [
        "agent_name",
        "image_url",
        "url",
        "player_code",
        "city_of_birth",
        "current_national_team_id"
    ]

    df = df.drop(*COLS_TO_DROP)

    print(f"{'-'*60}")
    # ==================================================================================================================
    # Convert Timestamp -> Date
    # ==================================================================================================================

    print("\n Convert Timestamp -> Date")
    df = (
        df
        .withColumn(
            "date_of_birth",
            F.to_date("date_of_birth")
        )
        .withColumn(
            "contract_expiration_date",
            F.to_date("contract_expiration_date")
        )
    )

    print(f"{'-'*60}")
    # ==================================================================================================================
    # Trim String Columns
    # ==================================================================================================================

    print("\n Trim String Columns")
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(
                col_name,
                F.trim(F.col(col_name))
            )

    print(f"{'-'*60}")
    # ==================================================================================================================
    # Standardize Values
    # ==================================================================================================================

    print("\n Standardize Values")
    df = df.withColumn(
        "position",
        F.when(
            F.col("position") == "Missing",
            "Unknown"
        ).otherwise(F.col("position"))
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Fill String Nulls
    # ==================================================================================================================

    print("\n Fill String Nulls")
    df = df.fillna({
        "country_of_birth": "Unknown",
        "country_of_citizenship": "Unknown",
        "sub_position": "Unknown",
        "foot": "Unknown",
        "current_club_domestic_competition_id": "Unknown",
        "current_club_name": "Unknown Club"
    })

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Fill Football Stats
    # ==================================================================================================================

    print("\n Fill Football Stats")
    df = df.fillna({
        "international_caps": 0,
        "international_goals": 0
    })

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Remove Duplicates
    # ==================================================================================================================

    print("\n Remove Duplicates")
    df = df.dropDuplicates(["player_id"])

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Handle Invalid Heights
    # ==================================================================================================================

    print("\n Handle Invalid Heights")
    df = df.withColumn(
        "height_in_cm",
        F.when(
            (F.col("height_in_cm") < 140) |
            (F.col("height_in_cm") > 220),
            None
        ).otherwise(F.col("height_in_cm"))
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Derived Columns
    # ==================================================================================================================

    print("\n Derived Columns")
    CURRENT_SEASON = (
        df
        .agg(F.max("last_season").alias("season"))
        .first()["season"]
    )

    df = (
        df

        .withColumn(
            "age",
            F.when(
                F.col("date_of_birth").isNotNull(),
                F.floor(
                    F.months_between(
                        F.current_date(),
                        F.col("date_of_birth")
                    ) / 12
                ).cast(IntegerType())
            )
        )

        .withColumn(
            "is_international",
            F.col("international_caps") > 0
        )

        .withColumn(
            "is_active",
            F.col("last_season") == CURRENT_SEASON
        )
    )

    print(f"{'-'*60}")


    # ==================================================================================================================
    # Save To S3
    # ==================================================================================================================

    print("\n================ Save Output to s3 as Parquet Files ================\n")

    df.write.mode("overwrite").parquet(S3_SILVER_PATH)

    print(f"\nSaved successfully to s3 bucket")


   



