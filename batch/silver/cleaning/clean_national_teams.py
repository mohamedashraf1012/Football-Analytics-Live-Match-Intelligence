# clean_national_teams.py


from pyspark.sql import functions as F
from pyspark.sql.types import *

TABLE = "national_teams"

# ==================================================================================================================
#  Schema — all columns read as StringType first.
#  Reason: safe handling of any empty strings before casting.
# ==================================================================================================================

national_teams_schema = StructType([
    StructField("national_team_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("team_code", StringType(), True),
    StructField("country_id", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("confederation", StringType(), True),
    StructField("team_image_url", StringType(), True),
    StructField("squad_size", StringType(), True),
    StructField("average_age", StringType(), True),
    StructField("foreigners_number", StringType(), True),
    StructField("foreigners_percentage", StringType(), True),
    StructField("total_market_value", StringType(), True),
    StructField("coach_name", StringType(), True),
    StructField("fifa_ranking", StringType(), True),
    StructField("last_season", StringType(), True),
    StructField("url", StringType(), True)
])



def clean_national_teams(spark, bucket):

    S3_BRONZE_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
    S3_SILVER_PATH = f"s3a://{bucket}/silver/{TABLE}"

    print("\n Read Bronze:")

    df = spark.read.csv(
        S3_BRONZE_PATH,
        header=True,
        schema=national_teams_schema
    )

    # cleaning
    print(f"\ncleaning script for {TABLE} table started")
    
    # ==================================================================================================================
    # Drop Unnecessary Columns
    # ==================================================================================================================

    print("\n Drop Unnecessary Columns")

    COLS_TO_DROP = [
        "coach_name",  # 100% nulls 
        "url",
        "team_image_url",
        "last_season",
        "team_code"  #redundant
    ]

    df = df.drop(*COLS_TO_DROP)

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
    # Safe Numeric Casting
    # ==================================================================================================================

    print("\n Safe Numeric Casting")

    df = (
        df
        .withColumn("national_team_id",      F.expr("try_cast(national_team_id as int)"))
        .withColumn("country_id",            F.expr("try_cast(country_id as int)"))
        .withColumn("squad_size",            F.expr("try_cast(squad_size as int)"))
        .withColumn("average_age",           F.expr("try_cast(average_age as double)"))
        .withColumn("foreigners_number",     F.expr("try_cast(foreigners_number as int)"))
        .withColumn("foreigners_percentage", F.expr("try_cast(foreigners_percentage as double)"))
        .withColumn("total_market_value",    F.expr("try_cast(total_market_value as bigint)"))
        .withColumn("fifa_ranking",          F.expr("try_cast(fifa_ranking as int)"))
    )

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Fill String Nulls
    # ==================================================================================================================

    print("\n Fill String Nulls")

    df = df.fillna({
        "confederation": "Unknown"
    })

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Remove Duplicates
    # ==================================================================================================================

    print("\n Remove Duplicates")

    df = df.dropDuplicates(["national_team_id"])

    print(f"{'-'*60}")


    # ==================================================================================================================
    # Save To S3
    # ==================================================================================================================

    print("\n================ Save Output to s3 as Parquet Files ================\n")

    df.write.mode("overwrite").parquet(S3_SILVER_PATH)

    print(f"\nSaved successfully to s3 bucket")




