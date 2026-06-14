# explore_transfers_table.py

from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DateType, DoubleType
)

# -------------------------------------------------
# 1. Create Spark Session + Read Data
# -------------------------------------------------
spark, bucket = get_spark("ExploreTransfers")

#---------------------------------------------------------------------------
TABLE = "transfers"  
S3_PATH=f"s3a://{bucket}/bronze/{TABLE}.csv"
#---------------------------------------------------------------------------

# All numeric columns read as StringType first to safely detect empty strings before casting.


transfers_schema = StructType([
    StructField("player_id", StringType(), True),

    StructField("transfer_date", StringType(), True),
    StructField("transfer_season", StringType(), True),

    StructField("from_club_id", StringType(), True),
    StructField("to_club_id", StringType(), True),

    StructField("from_club_name", StringType(), True),
    StructField("to_club_name", StringType(), True),

    StructField("transfer_fee", StringType(), True),
    StructField("market_value_in_eur", StringType(), True),

    StructField("player_name", StringType(), True)
])



#---------------------------------------------------------------------------

df = spark.read.csv(S3_PATH,
                    header=True, schema=transfers_schema)

print(f"\n{'='*60}")
print(f"  TABLE: {TABLE}.csv")
print(f"{'='*60}")

#---------------------------------------------------------------------------

# -------------------------------------------------
##  Force safe numeric cleaning
# -------------------------------------------------
df = df.withColumn(
    "market_value_in_eur",
    F.expr("try_cast(trim(market_value_in_eur) as double)")
)

df = df.withColumn(
    "transfer_fee",
    F.expr("try_cast(trim(transfer_fee) as double)")
)


# -------------------------------------------------
# 1. Total row count / schema
# -------------------------------------------------

print(f"\nTotal rows: {df.count():,}")
print("\nSchema:")
df.printSchema()


print(f"{'-'*60}")

# -------------------------------------------------
# 2. Sample Data
# -------------------------------------------------
print("\nFirst 3 rows:")
df.show(3, vertical=True, truncate=False)
print(f"{'-'*60}")

# -------------------------------------------------
# 3. Basic Column Check
# -------------------------------------------------
print("\nColumns:")
print(df.columns)

print(f"{'-'*60}")
# -------------------------------------------------
# 4. Missing Values Analysis
# -------------------------------------------------

string_cols = [
    "player_id",
    "transfer_date",
    "transfer_season",
    "from_club_id",
    "to_club_id",
    "from_club_name",
    "to_club_name",
    "player_name"
]

numeric_cols = [
    "transfer_fee",
    "market_value_in_eur"
]

print("\nMissing Values Per String Columns:")

missing_string_df = df.select([
    F.count(F.when(F.col(c).isNull() | (F.col(c) == ""), c)).alias(c)
    for c in string_cols
])

missing_string_df.show(vertical=True,truncate=False)

print("\nMissing Values Per Numeric Columns:")

missing_numeric_df = df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in numeric_cols
])
missing_numeric_df.show(vertical=True,truncate=False)



print(f"{'-'*60}")

# -------------------------------------------------
# 5. Duplicate Check
# -------------------------------------------------

print("\nDuplicate Rows Count:")

Duplicate_Rows_Count=df.groupBy(
    "player_id",
    "transfer_date",
    "from_club_id",
    "to_club_id"
).count().filter("count > 1")

Duplicate_Rows_Count.show(truncate=False)

print(f"{'-'*60}") 

# -------------------------------------------------
# 6. Date Field Analysis
# -------------------------------------------------
df = df.withColumn("transfer_date", F.to_date("transfer_date"))

print("\nDate Range:")
df.select(
    F.min("transfer_date").alias("min_date"),
    F.max("transfer_date").alias("max_date")
).show(truncate=False)

print(f"{'-'*60}") 
# -------------------------------------------------
# 7. Transfer Fee & Market Value Stats
# -------------------------------------------------
print("\nNumeric Summary:")

df.select(
    "transfer_fee",
    "market_value_in_eur"
).summary().show(truncate=False)  

print("\nget correct max, min of market value & transfer_fee:")

df.select(
    F.max("market_value_in_eur").alias("max_mv"),
    F.min("market_value_in_eur").alias("min_mv"),
    F.avg("market_value_in_eur").alias("avg_mv"),
    F.max("transfer_fee").alias("max_tf"),
    F.min("transfer_fee").alias("min_tf"),
    F.avg("transfer_fee").alias("avg_tf")
).show(truncate=False)


print(f"{'-'*60}") 

# -------------------------------------------------
# 8. Detect hidden bad formats in market_value_in_eur
# -------------------------------------------------
print("\nDetect hidden bad formats in market_value_in_eur:")

df.select("market_value_in_eur") \
  .filter(~F.col("market_value_in_eur").rlike("^[0-9.]+$")) \
  .show(20, truncate=False)

print(f"{'-'*60}") 

# -------------------------------------------------
# 9. Check missing values percentage
# -------------------------------------------------

print("\nNull percentage per column:")

total_rows = df.count()

null_percentages = df.select([
    (
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0))
        / F.lit(total_rows)
        * 100
    ).alias(c)
    for c in df.columns
])

null_percentages.show(vertical=True)

print(f"{'-'*60}")
# -------------------------------------------------
# 10. Descriptive statistics for numeric columns
# -------------------------------------------------

print("\nSummary statistics:")
df.describe().show(vertical=True, truncate=False)


spark.stop()
