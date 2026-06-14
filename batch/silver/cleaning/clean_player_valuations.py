# clean_player_valuations.py

from pyspark.sql import functions as F
from pyspark.sql.types import *

TABLE = "player_valuations"

# ==================================================================================================================
#  Schema — all columns read as StringType first.
#  Reason: safe handling of any empty strings before casting.
# ==================================================================================================================

player_valuations_schema = StructType([
    StructField("player_id",                            StringType(), True),
    StructField("date",                                 StringType(), True),
    StructField("market_value_in_eur",                  StringType(), True),
    StructField("current_club_name",                    StringType(), True),
    StructField("current_club_id",                      StringType(), True),
    StructField("player_club_domestic_competition_id",  StringType(), True),
])

def clean_player_valuations(spark, bucket):

    S3_BRONZE_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
    S3_SILVER_PATH = f"s3a://{bucket}/silver/{TABLE}"

    print("\n Read Bronze:")

    df = spark.read.csv(
        S3_BRONZE_PATH,
        header=True,
        schema=player_valuations_schema
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
        .withColumn("player_id",           F.expr("try_cast(player_id as int)"))
        .withColumn("current_club_id",     F.expr("try_cast(current_club_id as int)"))
        .withColumn("market_value_in_eur", F.expr("try_cast(market_value_in_eur as long)"))
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Handle Invalid market_value_in_eur
    # — exploration found 1 row with value = 0 (player 60096, 2008-03-17)
    # — a market value of 0 is invalid
    # ==================================================================================================================

    print("\n Handle Invalid market_value_in_eur")

    df = df.withColumn(
        "market_value_in_eur",
        F.when(F.col("market_value_in_eur") <= 0, None)
        .otherwise(F.col("market_value_in_eur"))
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Fill String Nulls
    # — player_club_domestic_competition_id: 14.1% null
    #   these are players without a tracked club or outside the 32 known leagues
    # — current_club_name: 0 nulls — "Unknown" and "Without Club" are already
    #   meaningful values in the data, no filling needed
    # ==================================================================================================================

    print("\n Fill String Nulls")

    df = df.fillna({
        "player_club_domestic_competition_id": "Unknown",
    })

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Remove Duplicates
    # — composite key: player_id + date
    # — exploration confirmed 0 duplicates, enforced defensively
    # ==================================================================================================================

    print("\n Remove Duplicates")

    df = df.dropDuplicates(["player_id", "date"])

    print(f"{'-'*60}")


    # ==================================================================================================================
    # Save To S3
    # ==================================================================================================================

    print("\n================ Save Output to s3 as Parquet Files ================\n")

    df.write.mode("overwrite").parquet(S3_SILVER_PATH)

    print(f"\nSaved successfully to s3 bucket")






