# clean_transfers.py

from pyspark.sql import functions as F
from pyspark.sql.types import *

TABLE = "transfers"

# ==================================================================================================================
#  Schema — all columns read as StringType first.
#  Reason: safe handling of any empty strings before casting.
# ==================================================================================================================

transfers_schema = StructType([
    StructField("player_id",            StringType(), True),
    StructField("transfer_date",        StringType(), True),
    StructField("transfer_season",      StringType(), True),
    StructField("from_club_id",         StringType(), True),
    StructField("to_club_id",           StringType(), True),
    StructField("from_club_name",       StringType(), True),
    StructField("to_club_name",         StringType(), True),
    StructField("transfer_fee",         StringType(), True),
    StructField("market_value_in_eur",  StringType(), True),
    StructField("player_name",          StringType(), True),
])



def clean_transfers(spark, bucket):

    S3_BRONZE_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
    S3_SILVER_PATH = f"s3a://{bucket}/silver/{TABLE}"

    print("\n Read Bronze:")

    df = spark.read.csv(
        S3_BRONZE_PATH,
        header=True,
        schema=transfers_schema
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
    # Safe Numeric Casting
    # — empty strings and bad formats become null (not errors)
    # — transfer_fee and market_value_in_eur are kept as LongType (whole euros,
    #   consistent with players.market_value_in_eur which is IntegerType)
    # ==================================================================================================================

    print("\n Safe Numeric Casting")

    df = (
        df
        .withColumn(
            "transfer_fee",
            F.expr("try_cast(transfer_fee as double)")
            .cast(LongType())
        )
        .withColumn(
            "market_value_in_eur",
            F.expr("try_cast(market_value_in_eur as double)")
            .cast(LongType())
        )
        .withColumn(
            "player_id",
            F.expr("try_cast(player_id as int)")
        )
        .withColumn(
            "from_club_id",
            F.expr("try_cast(from_club_id as int)")
        )
        .withColumn(
            "to_club_id",
            F.expr("try_cast(to_club_id as int)")
        )
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Convert transfer_date String -> DateType
    # ==================================================================================================================

    print("\n Convert transfer_date -> Date")

    df = df.withColumn(
        "transfer_date",
        F.to_date("transfer_date")
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Remove Duplicates
    # — composite key: player_id + transfer_date + from_club_id + to_club_id
    # — exploration confirmed 0 duplicates, but we enforce it defensively
    # ==================================================================================================================

    print("\n Remove Duplicates")

    df = df.dropDuplicates(["player_id", "transfer_date", "from_club_id", "to_club_id"])

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Standardize transfer_season Format
    # — raw values look like "25/26", "99/00", "00/01"
    # — already consistent; we just upper-case and null-fill unknowns
    # ==================================================================================================================

    print("\n Standardize transfer_season")

    df = df.withColumn(
        "transfer_season",
        F.when(
            F.col("transfer_season").isNull() | (F.col("transfer_season") == ""),
            "Unknown"
        ).otherwise(F.col("transfer_season"))
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Fill String Nulls
    # ==================================================================================================================

    print("\n Fill String Nulls")

    df = df.fillna({
        "from_club_name": "Unknown Club",
        "to_club_name":   "Unknown Club",
        "player_name":    "Unknown Player",
    })

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Fill Numeric Nulls
    # — transfer_fee null  → 0  (unknown/free transfer, consistent treatment)
    # — market_value_in_eur null → kept as null intentionally:
    #     a 0 market value is meaningful and different from "not recorded"
    #     downstream dbt models can handle nulls explicitly
    # ==================================================================================================================

    print("\n Fill Numeric Nulls")

    df = df.fillna({"transfer_fee": 0})

    print(f"{'-'*60}")



    # ==================================================================================================================
    # Save To S3
    # ==================================================================================================================

    print("\n================ Save Output to s3 as Parquet Files ================\n")

    df.write.mode("overwrite").parquet(S3_SILVER_PATH)

    print(f"\nSaved successfully to s3 bucket")




