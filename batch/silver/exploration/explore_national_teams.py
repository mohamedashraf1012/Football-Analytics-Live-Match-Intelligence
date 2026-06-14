# explore_national_teams_table.py

from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType
)

# -------------------------------------------------
# 1. Create Spark Session + Read Data
# -------------------------------------------------
spark, bucket = get_spark("ExploreNationalTeams")

#---------------------------------------------------------------------------
TABLE = "national_teams"
S3_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
#---------------------------------------------------------------------------
# All numeric columns read as StringType first to safely detect empty strings before casting.

#---------------------------------------------------------------------------

national_teams_schema = StructType([
    StructField("national_team_id",       StringType(), True),
    StructField("name",                   StringType(), True),
    StructField("team_code",              StringType(), True),
    StructField("country_id",             StringType(), True),
    StructField("country_name",           StringType(), True),
    StructField("country_code",           StringType(), True),
    StructField("confederation",          StringType(), True),
    StructField("team_image_url",         StringType(), True),
    StructField("squad_size",             StringType(), True),
    StructField("average_age",            StringType(), True),
    StructField("foreigners_number",      StringType(), True),
    StructField("foreigners_percentage",  StringType(), True),
    StructField("total_market_value",     StringType(), True),
    StructField("coach_name",             StringType(), True),
    StructField("fifa_ranking",           StringType(), True),
    StructField("last_season",            StringType(), True),
    StructField("url",                    StringType(), True),
])

#---------------------------------------------------------------------------

df = spark.read.csv(S3_PATH,
                    header=True, schema=national_teams_schema)

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

print("\nDuplicate check on national_team_id:")
dup_df = df.groupBy("national_team_id").count().filter("count > 1")
print(f"Duplicate national_team_id groups: {dup_df.count()}")
dup_df.show(10, truncate=False)

print(f"{'-'*60}")


# -------------------------------------------------
# 5. Numeric summary after safe casting
# -------------------------------------------------

df_cast = (
    df
    .withColumn("squad_size",            F.expr("try_cast(squad_size as int)"))
    .withColumn("average_age",           F.expr("try_cast(average_age as double)"))
    .withColumn("foreigners_number",     F.expr("try_cast(foreigners_number as int)"))
    .withColumn("foreigners_percentage", F.expr("try_cast(foreigners_percentage as double)"))
    .withColumn("total_market_value",    F.expr("try_cast(total_market_value as long)"))
    .withColumn("fifa_ranking",          F.expr("try_cast(fifa_ranking as int)"))
    .withColumn("last_season",           F.expr("try_cast(last_season as int)"))
)

print("\nNumeric summary:")
df_cast.select(
    "squad_size", "average_age", "foreigners_number",
    "foreigners_percentage", "total_market_value",
    "fifa_ranking", "last_season"
).summary().show(truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 6. Categorical distributions
# -------------------------------------------------

print("\nDistinct confederation values:")
df.groupBy("confederation") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(truncate=False)

print("\nlast_season distribution:")
df.groupBy("last_season") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 7. FIFA ranking check
# — check for duplicates (two teams can't share a rank)
# -------------------------------------------------

print("\nDuplicate FIFA rankings (should be empty):")
df_cast.groupBy("fifa_ranking") \
       .count() \
       .filter(F.col("count") > 1) \
       .orderBy("fifa_ranking") \
       .show(10, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 8. total_market_value inspection
# — plain integer here unlike clubs (no currency string)
# — check for zeros or negatives
# -------------------------------------------------

print("\nRows where total_market_value <= 0:")
df_cast.filter(
    F.col("total_market_value").isNotNull() &
    (F.col("total_market_value") <= 0)
).select("national_team_id", "name", "total_market_value") \
 .show(10, truncate=False)

print(f"\nTop 10 highest total_market_value:")
df_cast.orderBy(F.desc("total_market_value")) \
       .select("name", "total_market_value", "fifa_ranking") \
       .show(10, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 9. team_code vs name redundancy check
# — team_code is a slug version of name, likely always redundant
# -------------------------------------------------

print("\nRows where team_code is NOT a slug of name (sample):")
df.filter(
    F.col("team_code") != F.regexp_replace(F.lower(F.col("name")), r"\s+", "-")
).select("name", "team_code") \
 .show(20, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 10. Null percentage per column
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
# 11. Distinct count per column
# -------------------------------------------------

print("\nDistinct values count per column:")
distinct_counts = df.agg(*[
    F.countDistinct(F.col(c)).alias(c)
    for c in df.columns
])
distinct_counts.show(vertical=True)

print(f"{'-'*60}")

spark.stop()