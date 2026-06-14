# explore_games_table.py

from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType
)

# -------------------------------------------------
# 1. Create Spark Session + Read Data
# -------------------------------------------------
spark, bucket = get_spark("ExploreGames")

#---------------------------------------------------------------------------
TABLE = "games"
S3_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
#---------------------------------------------------------------------------

# All numeric columns read as StringType first to safely detect empty strings before casting.

#---------------------------------------------------------------------------

games_schema = StructType([
    StructField("game_id",                  StringType(), True),
    StructField("competition_id",           StringType(), True),
    StructField("season",                   StringType(), True),
    StructField("round",                    StringType(), True),
    StructField("date",                     StringType(), True),
    StructField("home_club_id",             StringType(), True),
    StructField("away_club_id",             StringType(), True),
    StructField("home_club_goals",          StringType(), True),
    StructField("away_club_goals",          StringType(), True),
    StructField("home_club_position",       StringType(), True),
    StructField("away_club_position",       StringType(), True),
    StructField("home_club_manager_name",   StringType(), True),
    StructField("away_club_manager_name",   StringType(), True),
    StructField("stadium",                  StringType(), True),
    StructField("attendance",               StringType(), True),
    StructField("referee",                  StringType(), True),
    StructField("url",                      StringType(), True),
    StructField("home_club_formation",      StringType(), True),
    StructField("away_club_formation",      StringType(), True),
    StructField("home_club_name",           StringType(), True),
    StructField("away_club_name",           StringType(), True),
    StructField("aggregate",               StringType(), True),
    StructField("competition_type",         StringType(), True),
])

#---------------------------------------------------------------------------

df = spark.read.csv(S3_PATH,
                    header=True, schema=games_schema)

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
# — null or empty string for every column
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
# 5. Duplicate Check on primary key
# -------------------------------------------------

print("\nDuplicate check on game_id:")
dup_df = df.groupBy("game_id").count().filter("count > 1")
print(f"Duplicate game_id groups: {dup_df.count()}")
dup_df.show(10, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 6. Safe numeric casting check
# — detect any values that fail to cast before we commit
# -------------------------------------------------

# numeric_cols = [
#     "game_id", "season", "home_club_id", "away_club_id",
#     "home_club_goals", "away_club_goals",
#     "home_club_position", "away_club_position",
#     "attendance",
# ]

# print("\nCast failure counts (values that cannot cast to int):")
# for c in numeric_cols:
#     failures = df.filter(
#         F.col(c).isNotNull() &
#         (F.trim(F.col(c)) != "") &
#         F.expr(f"try_cast({c} as int)").isNull()
#     ).count()
#     print(f"  {c}: {failures} cast failures")

# print(f"{'-'*60}")

# -------------------------------------------------
# 7. Numeric summary after safe casting
# -------------------------------------------------

df_cast = (
    df
    .withColumn("game_id",            F.expr("try_cast(game_id as int)"))
    .withColumn("season",             F.expr("try_cast(season as int)"))
    .withColumn("home_club_goals",    F.expr("try_cast(home_club_goals as int)"))
    .withColumn("away_club_goals",    F.expr("try_cast(away_club_goals as int)"))
    .withColumn("home_club_position", F.expr("try_cast(home_club_position as int)"))
    .withColumn("away_club_position", F.expr("try_cast(away_club_position as int)"))
    .withColumn("attendance",         F.expr("try_cast(attendance as int)"))
)

print("\nNumeric summary (after safe casting):")
df_cast.select(
    "season",
    "home_club_goals",
    "away_club_goals",
    "home_club_position",
    "away_club_position",
    "attendance",
).summary().show(truncate=False)

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
# 9. Categorical distributions
# -------------------------------------------------

print("\nDistinct values of 'competition_type':")
df.groupBy("competition_type") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(truncate=False)

print("\nTop 30 distinct values of 'round':")
df.groupBy("round") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(30, truncate=False)

print("\nTop 20 distinct values of 'home_club_formation':")
df.groupBy("home_club_formation") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(20, truncate=False)

print("\nSeason distribution:")
df.groupBy("season") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(30, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 10. aggregate column inspection
# — expected format "2:1" — check for anomalies
# -------------------------------------------------

print("\nDistinct aggregate formats (sample 20):")
df.select("aggregate") \
  .filter(F.col("aggregate").isNotNull() & (F.trim(F.col("aggregate")) != "")) \
  .distinct() \
  .orderBy("aggregate") \
  .show(20, truncate=False)

print("\nRows where aggregate does not match expected N:N pattern:")
df.filter(
    F.col("aggregate").isNotNull() &
    (F.trim(F.col("aggregate")) != "") &
    ~F.col("aggregate").rlike(r"^\d+:\d+$")
).select("game_id", "aggregate", "competition_type", "round") \
 .show(20, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 11. Goals sanity check
# — negative goals are invalid
# — very high scores worth flagging (>20 likely data error)
# -------------------------------------------------

print("\nGames with suspiciously high goals (home or away > 20):")
df_cast.filter(
    (F.col("home_club_goals") > 20) |
    (F.col("away_club_goals") > 20)
).select(
    "game_id", "home_club_name", "away_club_name",
    "home_club_goals", "away_club_goals", "date", "competition_id"
).show(20, truncate=False)

print("\nGames with negative goals:")
df_cast.filter(
    (F.col("home_club_goals") < 0) |
    (F.col("away_club_goals") < 0)
).select(
    "game_id", "home_club_goals", "away_club_goals"
).show(20, truncate=False)

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