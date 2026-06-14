# explore_club_games_table.py

from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType
)

# -------------------------------------------------
# 1. Create Spark Session + Read Data
# -------------------------------------------------
spark, bucket = get_spark("ExploreClubGames")

#---------------------------------------------------------------------------
TABLE = "club_games"
S3_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
#---------------------------------------------------------------------------


# All numeric columns read as StringType first to safely detect empty strings before casting.

#---------------------------------------------------------------------------

club_games_schema = StructType([
    StructField("game_id",                 StringType(), True),
    StructField("club_id",                 StringType(), True),
    StructField("own_goals",               StringType(), True),
    StructField("own_position",            StringType(), True),
    StructField("own_manager_name",        StringType(), True),
    StructField("opponent_id",             StringType(), True),
    StructField("opponent_goals",          StringType(), True),
    StructField("opponent_position",       StringType(), True),
    StructField("opponent_manager_name",   StringType(), True),
    StructField("hosting",                 StringType(), True),
    StructField("is_win",                  StringType(), True),
])

#---------------------------------------------------------------------------

df = spark.read.csv(S3_PATH,
                    header=True, schema=club_games_schema)

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
# 4. Duplicate Check on composite key
# -------------------------------------------------

print("\nDuplicate check on (game_id, club_id):")
dup_df = df.groupBy("game_id", "club_id").count().filter("count > 1")
print(f"Duplicate (game_id, club_id) groups: {dup_df.count()}")
dup_df.show(10, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 5. is_win distribution
# — expect 0 and 1 only, but draws could be encoded as 0
# — check all distinct values to understand encoding
# -------------------------------------------------

print("\nDistinct is_win values:")
df.groupBy("is_win") \
  .count() \
  .orderBy("is_win") \
  .show(truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 6. hosting distribution
# — expect "Home", "Away", possibly "Neutral"
# -------------------------------------------------

print("\nDistinct hosting values:")
df.groupBy("hosting") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(truncate=False)

print(f"{'-'*60}")



# -------------------------------------------------
# 7. Numeric summary after safe casting
# -------------------------------------------------

df_cast = (
    df
    .withColumn("own_goals",       F.expr("try_cast(own_goals as int)"))
    .withColumn("opponent_goals",  F.expr("try_cast(opponent_goals as int)"))
    .withColumn("own_position",    F.expr("try_cast(own_position as int)"))
    .withColumn("opponent_position", F.expr("try_cast(opponent_position as int)"))
    .withColumn("is_win",          F.expr("try_cast(is_win as int)"))
)

print("\nNumeric summary:")
df_cast.select(
    "own_goals", "opponent_goals",
    "own_position", "opponent_position", "is_win"
).summary().show(truncate=False)

print(f"{'-'*60}")


# -------------------------------------------------
# 8. Goals sanity check
# — negative goals invalid
# — cross-check: own_goals in club_games should match
#   home/away goals in games table (can verify after joining)
# -------------------------------------------------

print("\nRows with negative goals:")
df_cast.filter(
    (F.col("own_goals") < 0) | (F.col("opponent_goals") < 0)
).select("game_id", "club_id", "own_goals", "opponent_goals") \
 .show(10, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 9. Null percentage per column
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
# 10. Distinct count per column
# -------------------------------------------------

print("\nDistinct values count per column:")
distinct_counts = df.agg(*[
    F.countDistinct(F.col(c)).alias(c)
    for c in df.columns
])
distinct_counts.show(vertical=True)

print(f"{'-'*60}")

spark.stop()