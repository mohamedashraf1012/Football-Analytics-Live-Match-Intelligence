# clean_competitions.py

from pyspark.sql import functions as F
from pyspark.sql.types import *

TABLE = "competitions"

# ==================================================================================================================
#  Schema — all columns read as StringType first.
#  Reason: safe handling of any empty strings before casting.
# ==================================================================================================================


competitions_schema = StructType([
    StructField("competition_id",       StringType(), True),
    StructField("competition_code",     StringType(), True),
    StructField("name",                 StringType(), True),
    StructField("sub_type",             StringType(), True),
    StructField("type",                 StringType(), True),
    StructField("country_id",           StringType(), True),
    StructField("country_name",         StringType(), True),
    StructField("domestic_league_code", StringType(), True),
    StructField("confederation",        StringType(), True),
    StructField("total_clubs",          StringType(), True),
    StructField("url",                  StringType(), True),
])




def clean_competitions(spark, bucket):

    S3_BRONZE_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
    S3_SILVER_PATH = f"s3a://{bucket}/silver/{TABLE}"

    print("\n Read Bronze:")

    df = spark.read.csv(
        S3_BRONZE_PATH,
        header=True,
        schema=competitions_schema
    )

    # cleaning
    print(f"\ncleaning script for {TABLE} table started")

    # ==================================================================================================================
    # Drop Unnecessary Columns
    # — competition_code : URL slug, nearly identical to name 
    # — url              : web URL, no value in a data warehouse
    # ==================================================================================================================

    print("\n Drop Unnecessary Columns")

    COLS_TO_DROP = [
        "competition_code",
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
    # Handle country_id Sentinel
    # — -1 is used as a sentinel value for international competitions
    #   (UCL, World Cup, Copa America, etc.) that have no single country.
    # — Convert -1 -> null before casting so it reads as "no country"
    #   rather than a nonsense integer in Snowflake.
    # ==================================================================================================================

    print("\n Handle country_id Sentinel (-1 -> null)")

    df = df.withColumn(
        "country_id",
        F.when(F.col("country_id") == "-1", F.lit(None))
        .otherwise(F.col("country_id"))
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Safe Numeric Casting
    # ==================================================================================================================

    print("\n Safe Numeric Casting")

    df = (
        df
        .withColumn("country_id",   F.expr("try_cast(country_id as int)"))
        .withColumn("total_clubs",  F.expr("try_cast(total_clubs as int)"))
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Fill String Nulls
    # — country_name is null for international competitions — expected, fill with "International"
    # — domestic_league_code is null for purely international competitions — fill with "N/A"
    # ==================================================================================================================

    print("\n Fill String Nulls")

    df = df.fillna({
        "country_name":         "International",
        "domestic_league_code": "N/A",
    })

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Remove Duplicates
    # — exploration confirmed 0 duplicates on competition_id, enforced defensively
    # ==================================================================================================================

    print("\n Remove Duplicates")

    df = df.dropDuplicates(["competition_id"])

    print(f"{'-'*60}")



    # ==================================================================================================================
    # Save To S3
    # ==================================================================================================================

    print("\n================ Save Output to s3 as Parquet Files ================\n")

    df.write.mode("overwrite").parquet(S3_SILVER_PATH)

    print(f"\nSaved successfully to s3 bucket")


