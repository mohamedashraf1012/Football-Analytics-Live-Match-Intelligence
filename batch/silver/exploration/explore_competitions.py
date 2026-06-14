# explore_competitions_table.py

from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType
)

# -------------------------------------------------
# 1. Create Spark Session + Read Data
# -------------------------------------------------
spark, bucket = get_spark("ExploreCompetitions")

#---------------------------------------------------------------------------
TABLE = "competitions"
S3_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
#---------------------------------------------------------------------------
# All numeric columns read as StringType first to safely detect empty strings before casting.

#---------------------------------------------------------------------------

competitions_schema = StructType([
    StructField("competition_id",       StringType(), True),
    StructField("competition_code",     StringType(), True),
    StructField("name",                 StringType(), True),
    StructField("sub_type",             StringType(), True),
    StructField("type",                 StringType(), True),
    StructField("country_id",           StringType(), True),
    StructField("country_name",         StringType(), True),
    StructField("domestic_league_code", StringType(), True),
    StructField("confederation",        StringType(), True),
    StructField("total_clubs",          StringType(), True),
    StructField("url",                  StringType(), True),
])

#---------------------------------------------------------------------------

df = spark.read.csv(S3_PATH,
                    header=True, schema=competitions_schema)

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
# — check both null and empty string for every column
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

print("\nDuplicate check on competition_id:")
dup_df = df.groupBy("competition_id").count().filter("count > 1")
print(f"Duplicate competition_id groups: {dup_df.count()}")
dup_df.show(truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 6. Categorical distributions
# — type, sub_type, confederation are the key categoricals
# -------------------------------------------------

print("\nDistinct values of 'type':")
df.groupBy("type") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(truncate=False)

print("\nDistinct values of 'sub_type':")
df.groupBy("sub_type") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(truncate=False)

print("\nDistinct values of 'confederation':")
df.groupBy("confederation") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 7. country_id inspection
# — -1 is used as a sentinel for non-domestic competitions
# — check for any other unexpected values before casting
# -------------------------------------------------

print("\nDistinct country_id values:")
df.groupBy("country_id") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(30, truncate=False)

print("\nRows where country_id = -1 (international competitions):")
df.filter(F.col("country_id") == "-1") \
  .show(truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 8. Safe numeric casting check on country_id and total_clubs
# — try_cast lets us see how many fail
# -------------------------------------------------

df_cast = (
    df
    .withColumn("country_id_int",  F.expr("try_cast(country_id as int)"))
    .withColumn("total_clubs_int", F.expr("try_cast(total_clubs as int)"))
)

failed_country_id = df_cast.filter(
    F.col("country_id").isNotNull() &
    (F.col("country_id") != "") &
    F.col("country_id_int").isNull()
).count()

failed_total_clubs = df_cast.filter(
    F.col("total_clubs").isNotNull() &
    (F.col("total_clubs") != "") &
    F.col("total_clubs_int").isNull()
).count()

print(f"\ncountry_id cast failures  : {failed_country_id}")
print(f"total_clubs cast failures : {failed_total_clubs}")

print("\ntotal_clubs numeric summary (after safe cast):")
df_cast.select("total_clubs_int").summary().show(truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 9. domestic_league_code vs competition_id consistency
# — for domestic leagues, domestic_league_code should equal competition_id
# -------------------------------------------------

print("\nDomestic leagues where domestic_league_code != competition_id:")
df.filter(
    (F.col("type") == "domestic_league") &
    (F.col("domestic_league_code") != F.col("competition_id"))
).select("competition_id", "domestic_league_code", "name") \
 .show(truncate=False)

print("\nNon-domestic rows — domestic_league_code should be empty:")
df.filter(F.col("type") != "domestic_league") \
  .groupBy("domestic_league_code") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(truncate=False)

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

# -------------------------------------------------
# 12. Descriptive statistics for numeric columns
# -------------------------------------------------

print("\nSummary statistics:")
df.describe().show(vertical=True, truncate=False)


spark.stop()
