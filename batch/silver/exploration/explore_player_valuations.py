# explore_player_valuations_table.py

from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType
)

# -------------------------------------------------
# 1. Create Spark Session + Read Data
# -------------------------------------------------
spark, bucket = get_spark("ExplorePlayerValuations")

#---------------------------------------------------------------------------
TABLE = "player_valuations"
S3_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
#---------------------------------------------------------------------------
# All numeric columns read as StringType first to safely detect empty strings before casting.

#---------------------------------------------------------------------------

player_valuations_schema = StructType([
    StructField("player_id",                            StringType(), True),
    StructField("date",                                 StringType(), True),
    StructField("market_value_in_eur",                  StringType(), True),
    StructField("current_club_name",                    StringType(), True),
    StructField("current_club_id",                      StringType(), True),
    StructField("player_club_domestic_competition_id",  StringType(), True),
])

#---------------------------------------------------------------------------

df = spark.read.csv(S3_PATH,
                    header=True, schema=player_valuations_schema)

print(f"\n{'='*60}")
print(f"  TABLE: {TABLE}.csv")
print(f"{'='*60}")

# -------------------------------------------------
# 1. Total row count 
# -------------------------------------------------

print(f"\nTotal rows: {df.count():,}")
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

print("\nMissing Values (null or empty) per column:")

missing_df = df.select([
    F.count(
        F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), c)
    ).alias(c)
    for c in df.columns
])
missing_df.show(vertical=True, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 5. Duplicate Check
# — primary key is composite: player_id + date
#   (one valuation record per player per date)
# -------------------------------------------------

print("\nDuplicate check on (player_id, date):")
dup_df = df.groupBy("player_id", "date").count().filter("count > 1")
print(f"Duplicate (player_id, date) groups: {dup_df.count()}")
dup_df.show(10, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 6. Safe numeric casting check
# — detect any values that fail to cast before committing
# -------------------------------------------------

numeric_cols = ["player_id", "market_value_in_eur", "current_club_id"]

print("\nCast failure counts (values that cannot cast to int/long):")
for c in numeric_cols:
    failures = df.filter(
        F.col(c).isNotNull() &
        (F.trim(F.col(c)) != "") &
        F.expr(f"try_cast({c} as long)").isNull()
    ).count()
    print(f"  {c}: {failures} cast failures")

print(f"{'-'*60}")

# -------------------------------------------------
# 7. market_value_in_eur numeric summary
# -------------------------------------------------

df_cast = df.withColumn(
    "market_value_in_eur",
    F.expr("try_cast(market_value_in_eur as long)")
)

print("\nmarket_value_in_eur numeric summary:")
df_cast.select("market_value_in_eur").summary().show(truncate=False)

print("\nTop 10 highest market values:")
df_cast.orderBy(F.desc("market_value_in_eur")) \
       .select("player_id", "date", "market_value_in_eur", "current_club_name") \
       .show(10, truncate=False)

print("\nRows where market_value_in_eur <= 0:")
df_cast.filter(F.col("market_value_in_eur") <= 0) \
       .select("player_id", "date", "market_value_in_eur") \
       .show(10, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 8. Date field analysis
# -------------------------------------------------

df_dated = df.withColumn("date_parsed", F.to_date("date"))

print("\nDate range:")
df_dated.select(
    F.min("date_parsed").alias("min_date"),
    F.max("date_parsed").alias("max_date")
).show(truncate=False)

print("\nRows where date fails to parse:")
df_dated.filter(
    F.col("date").isNotNull() &
    (F.trim(F.col("date")) != "") &
    F.col("date_parsed").isNull()
).select("date").show(20, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 9. current_club_name inspection
# — "Unknown" already seen in sample rows, check prevalence
# -------------------------------------------------

print("\nTop 20 current_club_name values by count:")
df.groupBy("current_club_name") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(20, truncate=False)

print(f"\nRows where current_club_name = 'Unknown':")
unknown_clubs = df.filter(F.col("current_club_name") == "Unknown").count()
print(f"  {unknown_clubs:,}")

print(f"{'-'*60}")

# -------------------------------------------------
# 10. player_club_domestic_competition_id distribution
# — FK to competitions table, check for unexpected values
# -------------------------------------------------

print("\nDistinct player_club_domestic_competition_id values (top 30):")
df.groupBy("player_club_domestic_competition_id") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(30, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 11. Valuations per player distribution
# — how many valuation snapshots does a typical player have?
# -------------------------------------------------

print("\nValuations per player (summary):")
df.groupBy("player_id") \
  .count() \
  .select(
      F.min("count").alias("min_valuations"),
      F.max("count").alias("max_valuations"),
      F.avg("count").alias("avg_valuations"),
      F.expr("percentile(count, 0.5)").alias("median_valuations"),
  ).show(truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 12. Null percentage per column
# -------------------------------------------------

print("\nNull/empty percentage per column:")

total_rows = df.count()

null_percentages = df.select([
    (
        F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0))
        / F.lit(total_rows)
        * 100
    ).alias(c)
    for c in df.columns
])

null_percentages.show(vertical=True)

print(f"{'-'*60}")

# -------------------------------------------------
# 13. Distinct count per column
# -------------------------------------------------

print("\nDistinct values count per column:")
distinct_counts = df.agg(*[
    F.countDistinct(F.col(c)).alias(c)
    for c in df.columns
])
distinct_counts.show(vertical=True)

print(f"{'-'*60}")

spark.stop()