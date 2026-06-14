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
spark, bucket = get_spark("ExploreCountries")

#---------------------------------------------------------------------------
TABLE = "countries"
S3_PATH = f"s3a://{bucket}/bronze/{TABLE}.csv"
#---------------------------------------------------------------------------
# All numeric columns read as StringType first to safely detect empty strings before casting.

#---------------------------------------------------------------------------

countries_schema = StructType([
    StructField("country_id",                  StringType(), True),
    StructField("country_name",                StringType(), True),
    StructField("country_code",                StringType(), True),
    StructField("confederation",               StringType(), True),
    StructField("total_clubs",                 StringType(), True),
    StructField("total_players",               StringType(), True),
    StructField("average_age",                 StringType(), True),
    StructField("url",                         StringType(), True),

])

#---------------------------------------------------------------------------

df = spark.read.csv(S3_PATH,
                    header=True, schema=countries_schema)

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

print("\nDuplicate check on country_id:")
dup_df = df.groupBy("country_id").count().filter("count > 1")
print(f"Duplicate country_id groups: {dup_df.count()}")
dup_df.show(10, truncate=False)

print(f"{'-'*60}")


# -------------------------------------------------
# 5. Safe numeric casting check
# -------------------------------------------------

numeric_cols = [
    "country_id", "total_clubs", "total_players"
]

print("\nCast failure counts (values that cannot cast to int):")
for c in numeric_cols:
    failures = df.filter(
        F.col(c).isNotNull() &
        (F.trim(F.col(c)) != "") &
        F.expr(f"try_cast({c} as int)").isNull()
    ).count()
    print(f"  {c}: {failures} cast failures")


float_cols = [
    "average_age"
]

for c in float_cols:
    failures = df.filter(
        F.col(c).isNotNull() &
        (F.trim(F.col(c)) != "") &
        F.expr(f"try_cast({c} as double)").isNull()
    ).count()
    print(f"  {c}: {failures} cast failures")


print(f"{'-'*60}")

# -------------------------------------------------
# 6. Numeric summary after safe casting
# -------------------------------------------------

df_cast = (
    df
    .withColumn("country_id",          F.expr("try_cast(country_id as int)"))
    .withColumn("total_clubs",         F.expr("try_cast(total_clubs as int)"))
    .withColumn("total_players",       F.expr("try_cast(total_players as int)"))
    .withColumn("average_age",         F.expr("try_cast(average_age as double)"))
)

print("\nNumeric summary for stats columns:")
df_cast.select(
    "country_id", "total_clubs", "total_players", "average_age"
).summary().show(truncate=False)

print(f"{'-'*60}")



# -------------------------------------------------
# 7. Null percentage per column
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
# 8. Distinct count per column
# -------------------------------------------------

print("\nDistinct values count per column:")
distinct_counts = df.agg(*[
    F.countDistinct(F.col(c)).alias(c)
    for c in df.columns
])
distinct_counts.show(vertical=True)

print(f"{'-'*60}")

spark.stop()