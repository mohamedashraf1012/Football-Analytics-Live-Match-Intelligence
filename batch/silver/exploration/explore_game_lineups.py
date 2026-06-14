# explore_game_lineups_table.py

from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType
)

# -------------------------------------------------
# 1. Create Spark Session + Read Data
# -------------------------------------------------

spark, bucket = get_spark("ExploreGameLineups")

#---------------------------------------------------------------------------
TABLE = "game_lineups"
S3_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
#---------------------------------------------------------------------------

# All numeric columns read as StringType first to safely detect empty strings before casting.

game_lineups_schema = StructType([
    StructField("game_lineups_id", StringType(), True),
    StructField("date",            StringType(), True),
    StructField("game_id",         StringType(), True),
    StructField("player_id",       StringType(), True),
    StructField("club_id",         StringType(), True),
    StructField("player_name",     StringType(), True),
    StructField("type",            StringType(), True),
    StructField("position",        StringType(), True),
    StructField("number",          StringType(), True),
    StructField("team_captain",    StringType(), True),
])

#---------------------------------------------------------------------------

df = spark.read.csv(
    S3_PATH,
    header=True,
    schema=game_lineups_schema
)

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
        F.when(
            F.col(c).isNull() |
            (F.trim(F.col(c)) == ""),
            c
        )
    ).alias(c)
    for c in df.columns
])

missing_df.show(vertical=True, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 4. Duplicate Check on Primary Key
# -------------------------------------------------

print("\nDuplicate check on game_lineups_id:")

dup_df = (
    df.groupBy("game_lineups_id")
      .count()
      .filter("count > 1")
)

print(f"Duplicate game_lineups_id groups: {dup_df.count()}")

dup_df.show(10, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 5. Safe Numeric Casting
# -------------------------------------------------

numeric_cols = [
    "game_id",
    "player_id",
    "club_id",
    "number",
    "team_captain"
]

print("\nCast failure counts:")

for c in numeric_cols:
    failures = df.filter(
        F.col(c).isNotNull() &
        (F.trim(F.col(c)) != "") &
        F.expr(f"try_cast({c} as int)").isNull()
    ).count()

    print(f"  {c}: {failures}")

print(f"{'-'*60}")

# -------------------------------------------------
# 6. Numeric Summary
# -------------------------------------------------

df_cast = (
    df
    .withColumn("game_id",      F.expr("try_cast(game_id as int)"))
    .withColumn("player_id",    F.expr("try_cast(player_id as int)"))
    .withColumn("club_id",      F.expr("try_cast(club_id as int)"))
    .withColumn("number",       F.expr("try_cast(number as int)"))
    .withColumn("team_captain", F.expr("try_cast(team_captain as int)"))
)

print("\nNumeric Summary:")

df_cast.select(
    "number",
    "team_captain"
).summary().show(truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 7. Date Analysis
# -------------------------------------------------

df_date = df.withColumn(
    "date_parsed",
    F.to_date("date")
)

print("\nDate Range:")

df_date.select(
    F.min("date_parsed").alias("min_date"),
    F.max("date_parsed").alias("max_date")
).show(truncate=False)


print(f"{'-'*60}")

# -------------------------------------------------
# 8. Type Distribution
# -------------------------------------------------

print("\nType Distribution:")

df.groupBy("type") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 9. Position Distribution
# -------------------------------------------------

print("\nTop 20 Positions:")

df.groupBy("position") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(20, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 10. Invalid Captain Values
# -------------------------------------------------

print("\nRows where team_captain not in (0,1):")

df_cast.filter(
    ~F.col("team_captain").isin(0, 1)
).select(
    "game_lineups_id",
    "team_captain"
).show(20, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 11. Duplicate Player in Same Game
# -------------------------------------------------

print("\nPlayers appearing multiple times in same game:")

df.groupBy(
    "game_id",
    "player_id"
).count() \
 .filter(F.col("count") > 1) \
 .show(20, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 12. Null Percentage Per Column
# -------------------------------------------------

print("\nNull/empty percentage per column:")

total_rows = df.count()

null_percentages = df.select([
    (
        F.sum(
            F.when(
                F.col(c).isNull() |
                (F.trim(F.col(c)) == ""),
                1
            ).otherwise(0)
        )
        / F.lit(total_rows)
        * 100
    ).alias(c)
    for c in df.columns
])

null_percentages.show(vertical=True)

print(f"{'-'*60}")

# -------------------------------------------------
# 13. Distinct Count Per Column
# -------------------------------------------------

print("\nDistinct values count per column:")

distinct_counts = df.agg(*[
    F.countDistinct(F.col(c)).alias(c)
    for c in df.columns
])

distinct_counts.show(vertical=True)

print(f"{'-'*60}")

spark.stop()