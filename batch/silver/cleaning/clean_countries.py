# clean_countries.py

from pyspark.sql import functions as F
from pyspark.sql.types import *

TABLE = "countries"

# ==================================================================================================================
#  Schema — all columns read as StringType first.
#  Reason: safe handling of any empty strings before casting.
# ==================================================================================================================

countries_schema = StructType([
    StructField("country_id",    StringType(), True),
    StructField("country_name",  StringType(), True),
    StructField("country_code",  StringType(), True),
    StructField("confederation", StringType(), True),
    StructField("total_clubs",   StringType(), True),
    StructField("total_players", StringType(), True),
    StructField("average_age",   StringType(), True),
    StructField("url",           StringType(), True),
])


def clean_countries(spark, bucket):

    S3_BRONZE_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
    S3_SILVER_PATH = f"s3a://{bucket}/silver/{TABLE}"

    print("\n Read Bronze:")

    df = spark.read.csv(
        S3_BRONZE_PATH,
        header=True,
        schema=countries_schema
    )

    # cleaning
    print(f"\ncleaning script for {TABLE} table started")

    # ==================================================================================================================
    # Drop Unused Columns
    # — url: not needed 
    # ==================================================================================================================
    
    print("\n Drop Unused Columns")
    
    df = df.drop("url")
    
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
    # ==================================================================================================================

    print("\n Safe Numeric Casting")

    df = (
        df
        .withColumn("country_id",    F.expr("try_cast(country_id as int)"))
        .withColumn("total_clubs",   F.expr("try_cast(total_clubs as int)"))
        .withColumn("total_players", F.expr("try_cast(total_players as int)"))
        .withColumn("average_age",   F.expr("try_cast(average_age as double)"))
    )

    print(f"{'-'*60}")


    # ==================================================================================================================
    # Remove Duplicates
    # ==================================================================================================================

    print("\n Remove Duplicates")

    df = df.dropDuplicates(["country_id"])

    print(f"{'-'*60}")

    # ==================================================================================================================
    # Save To S3
    # ==================================================================================================================

    print("\n================ Save Output to s3 as Parquet Files ================\n")

    df.write.mode("overwrite").parquet(S3_SILVER_PATH)

    print(f"\nSaved successfully to s3 bucket")





