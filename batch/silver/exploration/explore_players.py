# explore_players_table.py
from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, TimestampType
)


# -------------------------------------------------
# 1. Create Spark Session + Read Data
# -------------------------------------------------

spark, bucket = get_spark("ExplorePlayers")

#---------------------------------------------------------------------------
TABLE = "players"  
PRIMARY_KEY="player_id"
S3_PATH=f"s3a://{bucket}/bronze/{TABLE}.csv"
#---------------------------------------------------------------------------

# All numeric columns read as StringType first to safely detect empty strings before casting.

players_schema = StructType([
    StructField("player_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("name", StringType(), True),
    StructField("last_season", IntegerType(), True),
    StructField("current_club_id", IntegerType(), True),
    StructField("player_code", StringType(), True),
    StructField("country_of_birth", StringType(), True),
    StructField("city_of_birth", StringType(), True),
    StructField("country_of_citizenship", StringType(), True),
    StructField("date_of_birth", TimestampType(), True),
    StructField("sub_position", StringType(), True),
    StructField("position", StringType(), True),
    StructField("foot", StringType(), True),
    StructField("height_in_cm", IntegerType(), True),
    StructField("contract_expiration_date", TimestampType(), True),
    StructField("agent_name", StringType(), True),
    StructField("image_url", StringType(), True),
    StructField("international_caps", IntegerType(), True),
    StructField("international_goals", IntegerType(), True),
    StructField("current_national_team_id", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("current_club_domestic_competition_id", StringType(), True),
    StructField("current_club_name", StringType(), True),
    StructField("market_value_in_eur", IntegerType(), True),
    StructField("highest_market_value_in_eur", IntegerType(), True)
])
#---------------------------------------------------------------------------

df = spark.read.csv(S3_PATH,
                    header=True, schema=players_schema)

print(f"\n{'='*60}")
print(f"  TABLE: {TABLE}.csv")
print(f"{'='*60}")
#---------------------------------------------------------------------------

# 1. Total row count

print(f"\nTotal rows: {df.count():,}")
print(f"{'-'*60}")
#---------------------------------------------------------------------------

# 2. Schema — column names and detected types

print("\nSchema:")
df.printSchema()

print(f"{'-'*60}")
#---------------------------------------------------------------------------

# 3. Null count for every column

print("\nNull counts per column:")
null_counts = df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df.columns
])
null_counts.show(vertical=True)

print(f"{'-'*60}")
#---------------------------------------------------------------------------

# 4. Sample rows
print("\nFirst 3 rows:")
df.show(3, vertical=True, truncate=False)
print(f"{'-'*60}")
#---------------------------------------------------------------------------

## 5. Duplicate check on primary key 

pk = PRIMARY_KEY   # table primary key
if pk in df.columns:
    total     = df.count()
    distinct  = df.dropDuplicates([pk]).count()
    dupes     = total - distinct
    print(f"\nDuplicate check on '{pk}': {dupes} duplicate rows")

print(f"{'-'*60}") 

#---------------------------------------------------------------------------

# 6. Detect empty strings


print("\nEmpty string counts:")

empty_counts = df.select([
    F.sum(
        F.when(F.trim(F.col(c).cast("string")) == "", 1)
         .otherwise(0)
    ).alias(c)
    for c in df.columns
])

empty_counts.show(vertical=True)

print(f"{'-'*60}")
#---------------------------------------------------------------------------

# 7. Distinct count per column


print("\nDistinct values count per column:")

distinct_counts = df.agg(*[
    F.countDistinct(F.col(c)).alias(c)
    for c in df.columns
])

distinct_counts.show(vertical=True)


print(f"{'-'*60}")

#---------------------------------------------------------------------------

# 8. Categorical value distribution check

def explore_categorical(df, col_name):
    print(f"\n===== {col_name} =====")
    
    df.select(F.lower(F.trim(F.col(col_name))).alias(col_name)) \
      .groupBy(col_name) \
      .count() \
      .orderBy(F.desc("count")) \
      .show(truncate=False)
    


print("\nCategorical value distribution check:")

explore_categorical(df, "foot")
explore_categorical(df, "position")

print(f"{'-'*60}")
#---------------------------------------------------------------------------

# 9. Descriptive statistics for numeric columns
print("\nSummary statistics:")
df.describe().show(vertical=True, truncate=False)
print(f"{'-'*60}")
#---------------------------------------------------------------------------

# 10. Check missing values percentage


print("\nNull percentage per column:")

total_rows = df.count()

null_percentages = df.select([
    (
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0))
        / F.lit(total_rows)
        * 100
    ).alias(c)
    for c in df.columns
])

null_percentages.show(vertical=True)

print(f"{'-'*60}")



#---------------------------------------------------------------------------
spark.stop()