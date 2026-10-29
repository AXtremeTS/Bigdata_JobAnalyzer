# etl/jobs/tang1.py
"""
Tang1 Ingestion Job:
Reads raw dataset directly from HDFS (hdfs://localhost:9000/raw/jobs/vietjobs.csv),
normalizes raw headers, injects ingestion metadata (_row_id, _source_file, _ingest_ts),
and writes to HDFS Tang1 layer in Parquet format.
"""

import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, current_timestamp, lit
import re

# Project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "etl"))

from jobmarket_etl.sniffer import normalize_column_name


def run_tang1_job(raw_hdfs_path: str = "hdfs://localhost:9000/raw/jobs/vietjobs.csv",
                   tang1_hdfs_path: str = "hdfs://localhost:9000/tang1/jobs"):
    print("=" * 70)
    print("  JOBMARKET PIPELINE — TANG1 INGESTION (SPARK ON HDFS)")
    print("=" * 70)
    start_time = time.time()

    # Configure Spark Session with HDFS
    spark = SparkSession.builder \
        .appName("JobMarket-Tang1-Ingestion") \
        .master("local[2]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/warehouse") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"\n[1] Reading raw CSV from HDFS: {raw_hdfs_path}")
    raw_df = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", "\"") \
        .csv(raw_hdfs_path)

    raw_count = raw_df.count()
    print(f"  [OK] Successfully loaded {raw_count:,} records from HDFS.")

    # Normalize column names to snake_case
    clean_columns = [normalize_column_name(c) for c in raw_df.columns]
    renamed_df = raw_df
    for old_col, new_col in zip(raw_df.columns, clean_columns):
        if old_col != new_col:
            renamed_df = renamed_df.withColumnRenamed(old_col, new_col)

    # Inject metadata columns
    print("\n[2] Injecting audit metadata (_row_id, _source_file, _ingest_ts)...")
    source_filename = raw_hdfs_path.split("/")[-1]
    tang1_df = renamed_df \
        .withColumn("_row_id", monotonically_increasing_id()) \
        .withColumn("_source_file", lit(source_filename)) \
        .withColumn("_ingest_ts", current_timestamp())

    print("\n[3] Writing Parquet to HDFS Tang1 layer:")
    print(f"  Target: {tang1_hdfs_path}")
    tang1_df.write \
        .mode("overwrite") \
        .parquet(tang1_hdfs_path)

    elapsed = round(time.time() - start_time, 2)
    print(f"\n[4] Tang1 Parquet written successfully in {elapsed}s.")
    print("  Tang1 Schema:")
    tang1_df.printSchema()

    spark.stop()
    print("=" * 70)
    print("  [SUCCESS] TANG1 STAGE COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    run_tang1_job()

