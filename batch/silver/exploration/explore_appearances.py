# explore_appearances_table.py

from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType
)

# -------------------------------------------------
# 1. Create Spark Session + Read Data
# -------------------------------------------------
spark, bucket = get_spark("ExploreAppearances")

#---------------------------------------------------------------------------
TABLE = "appearances"
S3_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
#---------------------------------------------------------------------------


# All numeric columns read as StringType first to safely detect empty strings before casting.

#---------------------------------------------------------------------------

appearances_schema = StructType([
    StructField("appearance_id",          StringType(), True),
    StructField("game_id",                StringType(), True),
    StructField("player_id",              StringType(), True),
    StructField("player_club_id",         StringType(), True),
    StructField("player_current_club_id", StringType(), True),
    StructField("date",                   StringType(), True),
    StructField("player_name",            StringType(), True),
    StructField("competition_id",         StringType(), True),
    StructField("yellow_cards",           StringType(), True),
    StructField("red_cards",              StringType(), True),
    StructField("goals",                  StringType(), True),
    StructField("assists",                StringType(), True),
    StructField("minutes_played",         StringType(), True),
])

#---------------------------------------------------------------------------

df = spark.read.csv(S3_PATH,
                    header=True, schema=appearances_schema)

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

print("\nDuplicate check on appearance_id:")
dup_df = df.groupBy("appearance_id").count().filter("count > 1")
print(f"Duplicate appearance_id groups: {dup_df.count()}")
dup_df.show(10, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 5. appearance_id format validation
# — expected format: "{game_id}_{player_id}"
# — verify it always matches this pattern
# -------------------------------------------------

print("\nRows where appearance_id does not match game_id_player_id pattern:")
df.filter(
    ~F.col("appearance_id").rlike(r"^\d+_\d+$")
).select("appearance_id", "game_id", "player_id") \
 .show(10, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 6. Safe numeric casting check
# -------------------------------------------------

numeric_cols = [
    "game_id", "player_id", "player_club_id", "player_current_club_id",
    "yellow_cards", "red_cards", "goals", "assists", "minutes_played"
]

print("\nCast failure counts (values that cannot cast to int):")
for c in numeric_cols:
    failures = df.filter(
        F.col(c).isNotNull() &
        (F.trim(F.col(c)) != "") &
        F.expr(f"try_cast({c} as int)").isNull()
    ).count()
    print(f"  {c}: {failures} cast failures")

print(f"{'-'*60}")

# -------------------------------------------------
# 7. Numeric summary after safe casting
# -------------------------------------------------

df_cast = (
    df
    .withColumn("yellow_cards",    F.expr("try_cast(yellow_cards as int)"))
    .withColumn("red_cards",       F.expr("try_cast(red_cards as int)"))
    .withColumn("goals",           F.expr("try_cast(goals as int)"))
    .withColumn("assists",         F.expr("try_cast(assists as int)"))
    .withColumn("minutes_played",  F.expr("try_cast(minutes_played as int)"))
)

print("\nNumeric summary for stats columns:")
df_cast.select(
    "yellow_cards", "red_cards", "goals", "assists", "minutes_played"
).summary().show(truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 8. Business logic sanity checks on stats
# — a player can have at most 2 yellow cards in one game
# — a player can have at most 1 red card in one game
# — goals and assists cannot be negative
# — minutes_played must be between 1 and 120 (extra time)
# -------------------------------------------------

print("\nRows with yellow_cards > 2:")
df_cast.filter(F.col("yellow_cards") > 2) \
       .select("appearance_id", "yellow_cards", "red_cards") \
       .show(10, truncate=False)

print("\nRows with red_cards > 1:")
df_cast.filter(F.col("red_cards") > 1) \
       .select("appearance_id", "yellow_cards", "red_cards") \
       .show(10, truncate=False)

print("\nRows with negative goals or assists:")
df_cast.filter(
    (F.col("goals") < 0) | (F.col("assists") < 0)
).select("appearance_id", "goals", "assists") \
 .show(10, truncate=False)

print("\nRows with minutes_played outside 1-120 range:")
df_cast.filter(
    (F.col("minutes_played") < 1) | (F.col("minutes_played") > 120)
).select("appearance_id", "minutes_played") \
 .show(10, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 9. Date field analysis
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
# 10. competition_id distribution
# -------------------------------------------------

print("\nTop 20 competition_id values by count:")
df.groupBy("competition_id") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(20, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 11. Goals and assists distribution
# -------------------------------------------------

print("\nGoals distribution:")
df_cast.groupBy("goals") \
       .count() \
       .orderBy("goals") \
       .show(truncate=False)

print("\nAssists distribution:")
df_cast.groupBy("assists") \
       .count() \
       .orderBy("assists") \
       .show(truncate=False)

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