# explore_game_events.py

from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType
)

# -------------------------------------------------
# 1. Create Spark Session + Read Data
# -------------------------------------------------
spark, bucket = get_spark("ExploreGameEvents")

#---------------------------------------------------------------------------
TABLE = "game_events"
S3_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
#---------------------------------------------------------------------------
# All numeric columns read as StringType first to safely detect empty strings before casting.

#---------------------------------------------------------------------------

game_events_schema = StructType([
    StructField("game_event_id",    StringType(), True),
    StructField("date",             StringType(), True),
    StructField("game_id",          StringType(), True),
    StructField("minute",           StringType(), True),
    StructField("type",             StringType(), True),
    StructField("club_id",          StringType(), True),
    StructField("club_name",        StringType(), True),
    StructField("player_id",        StringType(), True),
    StructField("description",      StringType(), True),
    StructField("player_in_id",     StringType(), True),
    StructField("player_assist_id", StringType(), True),
])

#---------------------------------------------------------------------------

df = spark.read.csv(S3_PATH,
                    header=True, schema=game_events_schema)

print(f"\n{'='*60}")
print(f"  TABLE: {TABLE}.csv")
print(f"{'='*60}")

# -------------------------------------------------
# 1. Total row count
# -------------------------------------------------

print(f"\nTotal rows: {df.count():,}")

print(f"{'-'*60}")

# -------------------------------------------------
# 2. Basic Column Check
# -------------------------------------------------

print("\nColumns:")
print(df.columns)
print(f"{'-'*60}")



# -------------------------------------------------
# 3. Missing Values Analysis
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
# 4. Duplicate Check on primary key
# -------------------------------------------------

print("\nDuplicate check on game_event_id:")
dup_df = df.groupBy("game_event_id").count().filter("count > 1")

print(f"Duplicate game_event_id groups: {dup_df.count()}")


print(f"{'-'*60}")

# -------------------------------------------------
# 5. game_event_id format validation
# — expected: 32-character lowercase hex string 
# -------------------------------------------------

print("\nRows where game_event_id does not match 32-char hex pattern:")
df.filter(
    F.col("game_event_id").isNotNull() &
    ~F.col("game_event_id").rlike(r"^[a-f0-9]{32}$")
).select("game_event_id") \
 .show(10, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 6. type — list all distinct values to catch unexpected entries
# -------------------------------------------------

print("\nAll distinct type values:")
df.groupBy("type") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 7. Safe numeric casting check
# -------------------------------------------------

print("\nCast failure counts (values that cannot cast to their target type):")

for c in ["game_id", "club_id", "player_id", "player_in_id", "player_assist_id"]:
    failures = df.filter(
        F.col(c).isNotNull() &
        (F.trim(F.col(c)) != "") &
        F.expr(f"try_cast({c} as int)").isNull()
    ).count()
    print(f"  {c} -> int: {failures} cast failures")

# minute — could be int but may contain "90+2" style strings
minute_failures = df.filter(
    F.col("minute").isNotNull() &
    (F.trim(F.col("minute")) != "") &
    F.expr("try_cast(minute as int)").isNull()
).count()
print(f"  minute -> int: {minute_failures} cast failures")

print(f"{'-'*60}")

# -------------------------------------------------
# 8. minute — range and format analysis
# — standard range: 1–90; extra time: up to 120+
# — check for non-integer formats like "90+2"
# -------------------------------------------------

print("\nSample of non-integer minute values (if any):")
df.filter(
    F.col("minute").isNotNull() &
    (F.trim(F.col("minute")) != "") &
    F.expr("try_cast(minute as int)").isNull()
).select("game_event_id", "minute", "type") \
 .show(20, truncate=False)

df_cast = (
    df
    .withColumn("minute",           F.expr("try_cast(minute as int)"))
    .withColumn("game_id",          F.expr("try_cast(game_id as int)"))
    .withColumn("club_id",          F.expr("try_cast(club_id as int)"))
    .withColumn("player_id",        F.expr("try_cast(player_id as int)"))
    .withColumn("player_in_id",     F.expr("try_cast(player_in_id as int)"))
    .withColumn("player_assist_id", F.expr("try_cast(player_assist_id as int)"))
)

print("\nminute numeric summary:")
df_cast.select("minute").summary().show(truncate=False)

print("\nRows with minute < 1:")
df_cast.filter(F.col("minute") < 1) \
       .select("game_event_id", "minute", "type") \
       .show(10, truncate=False)

print("\nRows with minute > 120:")
df_cast.filter(F.col("minute") > 120) \
       .select("game_event_id", "minute", "type") \
       .show(10, truncate=False)

print(f"{'-'*60}")



# -------------------------------------------------
# 9. description — length distribution
# -------------------------------------------------


print("\ndescription length distribution (percentiles):")
df.select(F.length("description").alias("desc_length")) \
  .summary() \
  .show(truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 10. Date field analysis
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
).select("game_event_id", "date") \
 .show(10, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 11. Null / empty percentage per column
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
# 12. Distinct count per column
# -------------------------------------------------

print("\nDistinct values count per column:")
distinct_counts = df.agg(*[
    F.countDistinct(F.col(c)).alias(c)
    for c in df.columns
])
distinct_counts.show(vertical=True)

print(f"{'-'*60}")

spark.stop()