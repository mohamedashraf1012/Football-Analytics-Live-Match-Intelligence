# explore_clubs_table.py

from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType
)

# -------------------------------------------------
# 1. Create Spark Session + Read Data
# -------------------------------------------------
spark, bucket = get_spark("ExploreClubs")

#---------------------------------------------------------------------------
TABLE = "clubs"
S3_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
#---------------------------------------------------------------------------
# All numeric columns read as StringType first to safely detect empty strings before casting.

#---------------------------------------------------------------------------

clubs_schema = StructType([
    StructField("club_id",                  StringType(), True),
    StructField("club_code",                StringType(), True),
    StructField("name",                     StringType(), True),
    StructField("domestic_competition_id",  StringType(), True),
    StructField("total_market_value",       StringType(), True),
    StructField("squad_size",               StringType(), True),
    StructField("average_age",              StringType(), True),
    StructField("foreigners_number",        StringType(), True),
    StructField("foreigners_percentage",    StringType(), True),
    StructField("national_team_players",    StringType(), True),
    StructField("stadium_name",             StringType(), True),
    StructField("stadium_seats",            StringType(), True),
    StructField("net_transfer_record",      StringType(), True),
    StructField("coach_name",               StringType(), True),
    StructField("last_season",              StringType(), True),
    StructField("filename",                 StringType(), True),
    StructField("url",                      StringType(), True),
])

#---------------------------------------------------------------------------

df = spark.read.csv(S3_PATH,
                    header=True, schema=clubs_schema)

print(f"\n{'='*60}")
print(f"  TABLE: {TABLE}.csv")
print(f"{'='*60}")

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
print("\nFirst 5 rows:")
df.show(5, vertical=True, truncate=False)
print(f"{'-'*60}")

# -------------------------------------------------
# 3. Basic Column Check
# -------------------------------------------------
print("\nColumns:")
print(df.columns)

print(f"{'-'*60}")

# -------------------------------------------------
# 4. Missing Values Analysis
# — split into string cols and numeric-string cols
#   because numeric cols need null OR empty string check
# -------------------------------------------------

string_cols = [
    "club_id",
    "club_code",
    "name",
    "domestic_competition_id",
    "stadium_name",
    "coach_name",
    "last_season",
    "filename",
    "url",
]

numeric_string_cols = [
    "total_market_value",
    "squad_size",
    "average_age",
    "foreigners_number",
    "foreigners_percentage",
    "national_team_players",
    "stadium_seats",
    "net_transfer_record",
]

print("\nMissing Values (null or empty) — String Columns:")
missing_string_df = df.select([
    F.count(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), c)).alias(c)
    for c in string_cols
])
missing_string_df.show(vertical=True, truncate=False)

print("\nMissing Values (null or empty) — Numeric-String Columns:")
missing_numeric_df = df.select([
    F.count(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), c)).alias(c)
    for c in numeric_string_cols
])
missing_numeric_df.show(vertical=True, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 5. Duplicate Check
# — primary key is club_id
# -------------------------------------------------

print("\nDuplicate check on club_id:")
dup_df = df.groupBy("club_id").count().filter("count > 1")
print(f"Duplicate club_id groups: {dup_df.count()}")
dup_df.show(10, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 6. Inspect currency columns — raw format samples
# — total_market_value and net_transfer_record need
#   manual parsing; see what formats actually exist
# -------------------------------------------------

print("\nDistinct formats in total_market_value (sample 20):")
df.select("total_market_value") \
  .filter(F.col("total_market_value").isNotNull() & (F.col("total_market_value") != "")) \
  .distinct() \
  .orderBy("total_market_value") \
  .show(20, truncate=False)

print("\nDistinct formats in net_transfer_record (sample 30):")
df.select("net_transfer_record") \
  .filter(F.col("net_transfer_record").isNotNull() & (F.col("net_transfer_record") != "")) \
  .distinct() \
  .orderBy("net_transfer_record") \
  .show(30, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 7. Check for unexpected suffixes in currency cols
#    We expect "€", "m", "bn", "+", "-" characters only
# -------------------------------------------------

print("\nRows where total_market_value contains unexpected characters:")
df.filter(
    F.col("total_market_value").isNotNull() &
    ~F.col("total_market_value").rlike(r"^[+\-€0-9.,mbn\s]+$")
).select("total_market_value").show(20, truncate=False)

print("\nRows where net_transfer_record contains unexpected characters:")
df.filter(
    F.col("net_transfer_record").isNotNull() &
    ~F.col("net_transfer_record").rlike(r"^[+\-€0-9.,mbn\s]+$")
).select("net_transfer_record").show(20, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 8. Safe numeric casting for clean numeric columns
# -------------------------------------------------

df_num = (
    df
    .withColumn("squad_size",            F.expr("try_cast(trim(squad_size) as int)"))
    .withColumn("average_age",           F.expr("try_cast(trim(average_age) as double)"))
    .withColumn("foreigners_number",     F.expr("try_cast(trim(foreigners_number) as int)"))
    .withColumn("foreigners_percentage", F.expr("try_cast(trim(foreigners_percentage) as double)"))
    .withColumn("national_team_players", F.expr("try_cast(trim(national_team_players) as int)"))
    .withColumn("stadium_seats",         F.expr("try_cast(trim(stadium_seats) as int)"))
    .withColumn("last_season",           F.expr("try_cast(trim(last_season) as int)"))
    .withColumn("club_id",               F.expr("try_cast(trim(club_id) as int)"))
)

# -------------------------------------------------
# 9. Numeric Summary for castable columns
# -------------------------------------------------

print("\nNumeric Summary (after safe casting):")
df_num.select(
    "squad_size",
    "average_age",
    "foreigners_number",
    "foreigners_percentage",
    "national_team_players",
    "stadium_seats",
    "last_season",
).summary().show(truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 10. Categorical distribution
# -------------------------------------------------

print("\nDistinct domestic_competition_id values (top 30 by count):")
df.groupBy("domestic_competition_id") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(30, truncate=False)

print("\nlast_season distribution:")
df.groupBy("last_season") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(30, truncate=False)

print(f"{'-'*60}")

# -------------------------------------------------
# 11. Null percentage per column
# -------------------------------------------------

print("\nNull percentage per column:")

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