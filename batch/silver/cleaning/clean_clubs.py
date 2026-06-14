
# clean_clubs.py

from pyspark.sql import functions as F
from pyspark.sql.types import *

TABLE = "clubs"

# ==================================================================================================================
#  Schema — all columns read as StringType first.
#  Reason: safe handling of any empty strings before casting.
# ==================================================================================================================

clubs_schema = StructType([
    StructField("club_id",                  StringType(), True),
    StructField("club_code",                StringType(), True),
    StructField("name",                     StringType(), True),
    StructField("domestic_competition_id",  StringType(), True),
    StructField("total_market_value",       StringType(), True),
    StructField("squad_size",               StringType(), True),
    StructField("average_age",              StringType(), True),
    StructField("foreigners_number",        StringType(), True),
    StructField("foreigners_percentage",    StringType(), True),
    StructField("national_team_players",    StringType(), True),
    StructField("stadium_name",             StringType(), True),
    StructField("stadium_seats",            StringType(), True),
    StructField("net_transfer_record",      StringType(), True),
    StructField("coach_name",               StringType(), True),
    StructField("last_season",              StringType(), True),
    StructField("filename",                 StringType(), True),
    StructField("url",                      StringType(), True),
])


def clean_clubs(spark, bucket):

    S3_BRONZE_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
    S3_SILVER_PATH = f"s3a://{bucket}/silver/{TABLE}"

    print("\n Read Bronze:")

    df = spark.read.csv(
        S3_BRONZE_PATH,
        header=True,
        schema=clubs_schema
    )

    # cleaning
    print(f"\ncleaning script for {TABLE} table started")

    # ==================================================================================================================
    # Drop Unnecessary Columns
    # — total_market_value : 100% null across all 796 rows, zero information
    # — coach_name         : 88.7% null, not useful for data modeling
    # — club_code          : redundant column, club_id is the key
    # — filename           : local path, not data
    # — url                : web URL, no value in a data warehouse
    # ==================================================================================================================

    print("\n Drop Unnecessary Columns")

    COLS_TO_DROP = [
        "total_market_value",
        "coach_name",
        "club_code",
        "filename",
        "url",
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
    # Safe Numeric Casting
    # — empty strings and unparseable values become null rather than errors
    # ==================================================================================================================

    print("\n Safe Numeric Casting")

    df = (
        df
        .withColumn("club_id",                F.expr("try_cast(club_id as int)"))
        .withColumn("squad_size",             F.expr("try_cast(squad_size as int)"))
        .withColumn("average_age",            F.expr("try_cast(average_age as double)"))
        .withColumn("foreigners_number",      F.expr("try_cast(foreigners_number as int)"))
        .withColumn("foreigners_percentage",  F.expr("try_cast(foreigners_percentage as double)"))
        .withColumn("national_team_players",  F.expr("try_cast(national_team_players as int)"))
        .withColumn("stadium_seats",          F.expr("try_cast(stadium_seats as int)"))
        .withColumn("last_season",            F.expr("try_cast(last_season as int)"))
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Parse net_transfer_record -> LongType (whole euros)
    #
    # Four raw formats observed in exploration:
    #   "+€5.90m"  -> positive millions  ->  +5_900_000
    #   "€-72.30m" -> negative millions  -> -72_300_000  
    #   "+€900k"   -> positive thousands ->   +900_000
    #   "€-131k"   -> negative thousands ->   -131_000   
    #   "+-0"      -> zero               ->          0

    # ==================================================================================================================

    print("\n Parse net_transfer_record")

    df = df.withColumn(
        "net_transfer_record_eur",
        F.when(
            F.col("net_transfer_record").isNull() | (F.col("net_transfer_record") == ""),
            F.lit(None).cast(LongType())
        )
        .when(
            F.col("net_transfer_record") == "+-0",
            F.lit(0).cast(LongType())
        )
        .otherwise(
            (
                # sign
                F.when(F.col("net_transfer_record").contains("€-"), F.lit(-1))
                .otherwise(F.lit(1))
                *
                # numeric digits only
                F.regexp_replace(F.col("net_transfer_record"), r"[^0-9.]", "")
                .cast(DoubleType())
                *
                # unit multiplier
                F.when(F.lower(F.col("net_transfer_record")).endswith("m"), F.lit(1_000_000))
                .when(F.lower(F.col("net_transfer_record")).endswith("k"), F.lit(1_000))
                .otherwise(F.lit(1))
            ).cast(LongType())
        )
    ).drop("net_transfer_record")

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Handle Invalid Numeric Ranges
    # — squad_size    : 0 is not a valid squad size    -> null
    # — stadium_seats : 0 is not a valid seat capacity -> null
    # ==================================================================================================================

    print("\n Handle Invalid Numeric Ranges")

    df = (
        df
        .withColumn(
            "squad_size",
            F.when(F.col("squad_size") <= 0, None)
            .otherwise(F.col("squad_size"))
        )
        .withColumn(
            "stadium_seats",
            F.when(F.col("stadium_seats") <= 0, None)
            .otherwise(F.col("stadium_seats"))
        )
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Remove Duplicates
    # — exploration confirmed 0 duplicates on club_id, enforced defensively
    # ==================================================================================================================

    print("\n Remove Duplicates")

    df = df.dropDuplicates(["club_id"])

    print(f"{'-'*60}")
    

    # ==================================================================================================================
    # Save To S3
    # ==================================================================================================================

    print("\n================ Save Output to s3 as Parquet Files ================\n")

    df.write.mode("overwrite").parquet(S3_SILVER_PATH)

    print(f"\nSaved successfully to s3 bucket")


