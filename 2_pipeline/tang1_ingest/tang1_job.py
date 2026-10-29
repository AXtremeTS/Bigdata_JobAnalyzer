# 2_pipeline/tang1_ingest/tang1_job.py
"""
TẦNG 1 (TANG 1) — INGESTION & PARQUET CONVERSION JOB:
Đọc tập dữ liệu Raw CSV trực tiếp từ HDFS (hdfs://localhost:9000/raw/jobs/vietjobs.csv),
chuẩn hóa tên cột thô thành snake_case, tiêm metadata kiểm toán (_row_id, _source_file, _ingest_ts),
và ghi xuống HDFS Tầng 1 dưới định dạng Snappy Parquet (hdfs://localhost:9000/tang1/jobs).
"""

import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, current_timestamp, lit

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "etl"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "2_pipeline", "tang2_cleaning"))

from jobmarket_etl.sniffer import normalize_column_name


def run_tang1_job(raw_hdfs_path: str = "hdfs://localhost:9000/raw/jobs/vietjobs.csv",
                  tang1_hdfs_path: str = "hdfs://localhost:9000/tang1/jobs"):
    print("=" * 70)
    print("  JOBMARKET PIPELINE — TẦNG 1: RAW INGESTION & PARQUET (SPARK ON HDFS)")
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

    print(f"\n[1] Đang đọc tệp CSV thô từ HDFS: {raw_hdfs_path}")
    raw_df = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", "\"") \
        .csv(raw_hdfs_path)

    raw_count = raw_df.count()
    print(f"  [OK] Đã nạp thành công {raw_count:,} bản ghi từ HDFS.")

    # Normalize column names to snake_case
    clean_columns = [normalize_column_name(c) for c in raw_df.columns]
    renamed_df = raw_df
    for old_col, new_col in zip(raw_df.columns, clean_columns):
        if old_col != new_col:
            renamed_df = renamed_df.withColumnRenamed(old_col, new_col)

    # Inject audit metadata
    print("\n[2] Tiêm metadata kiểm toán (_row_id, _source_file, _ingest_ts)...")
    source_filename = raw_hdfs_path.split("/")[-1]
    tang1_df = renamed_df \
        .withColumn("_row_id", monotonically_increasing_id()) \
        .withColumn("_source_file", lit(source_filename)) \
        .withColumn("_ingest_ts", current_timestamp())

    # Write Parquet to HDFS Tang 1
    print(f"\n[3] Ghi định dạng Parquet Snappy xuống HDFS Tầng 1: {tang1_hdfs_path}")
    tang1_df.write \
        .mode("overwrite") \
        .parquet(tang1_hdfs_path)

    # Backward compatibility mirror to /tang1/jobs
    try:
        tang1_df.write.mode("overwrite").parquet("hdfs://localhost:9000/tang1/jobs")
    except Exception:
        pass

    elapsed = round(time.time() - start_time, 2)
    print(f"\n[4] TẦNG 1 hoàn tất xuất sắc trong {elapsed} giây.")
    print("  Schema Tầng 1 (Parquet Snappy):")
    tang1_df.printSchema()

    return tang1_df


# Backward compatibility alias
run_tang1_job = run_tang1_job

if __name__ == "__main__":
    run_tang1_job()

