#spark_session.py

import os
from pyspark.sql import SparkSession

# ── import aws credentials from config.py ──────────────────────────────────────
from config import AWS_ACCESS_KEY,AWS_SECRET_KEY,AWS_REGION,S3_BUCKET

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"


def get_spark(app_name="FootballFlow"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.2") \
        .config("spark.hadoop.fs.s3a.access.key",    AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key",    AWS_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint",      f"s3.{AWS_REGION}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl",          "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.memory",               "8g") \
        .config("spark.sql.shuffle.partitions", "12") \
        .config("spark.default.parallelism", "12") \
        .config("spark.sql.adaptive.enabled", "true")\
        .config("spark.local.dir", "D:/spark-temp") \
        .config("spark.hadoop.hadoop.tmp.dir", "D:/spark-temp") \
        .config("spark.hadoop.fs.s3a.buffer.dir", "D:/spark-temp") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark, S3_BUCKET

