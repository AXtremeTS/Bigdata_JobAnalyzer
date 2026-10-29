# etl/jobs/profiler_job.py
"""
Profile & Adaptive Mapping Job:
Reads Tang1 dataset from HDFS, profiles column null rates and distinct values,
runs semantic schema mapping, and detects market characteristics (country, currency, language).
Outputs profile.json, mapping.json, market.json.
"""

import sys
import os
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, approx_count_distinct

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "etl"))

from jobmarket_etl.mapper import generate_schema_mapping
from jobmarket_etl.market_detector import detect_market


def run_profile_job(tang1_hdfs_path: str = "hdfs://localhost:9000/tang1/jobs",
                    output_meta_dir: str = os.path.join(PROJECT_ROOT, "data", "meta")):
    print("=" * 70)
    print("  JOBMARKET PIPELINE — DATASET PROFILING & ADAPTIVE SCHEMA")
    print("=" * 70)
    start_time = time.time()

    os.makedirs(output_meta_dir, exist_ok=True)

    spark = SparkSession.builder \
        .appName("JobMarket-Profile-Job") \
        .master("local[2]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"\n[1] Loading Tang1 dataset from HDFS: {tang1_hdfs_path}")
    df = spark.read.parquet(tang1_hdfs_path)
    total_rows = df.count()
    print(f"  Total records: {total_rows:,} rows")

    data_columns = [c for c in df.columns if not c.startswith("_")]
    print(f"  Analyzing {len(data_columns)} data columns: {', '.join(data_columns)}")

    # 1. Null Counts & Distinct Approximations
    print("\n[2] Computing column statistics (null rates & approx distinct)...")
    exprs = []
    for c in data_columns:
        exprs.append(count(when(col(c).isNull() | (col(c) == "") | (col(c) == "null") | (col(c) == "N/A"), 1)).alias(f"{c}_nulls"))
        exprs.append(approx_count_distinct(col(c)).alias(f"{c}_distinct"))

    stats_row = df.select(exprs).collect()[0]

    columns_profile = {}
    samples_by_col = {}

    for c in data_columns:
        null_count = stats_row[f"{c}_nulls"]
        distinct_count = stats_row[f"{c}_distinct"]
        null_pct = round((null_count / float(total_rows)) * 100.0, 2)

        # Collect top 5 distinct sample values
        samples = [
            str(r[0]) for r in df.filter(col(c).isNotNull() & (col(c) != "")).select(c).distinct().limit(5).collect()
        ]
        samples_by_col[c] = samples

        columns_profile[c] = {
            "null_count": null_count,
            "null_pct": null_pct,
            "approx_distinct": distinct_count,
            "sample_values": samples
        }

    profile_result = {
        "total_records": total_rows,
        "column_count": len(data_columns),
        "columns": columns_profile,
        "profiled_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

    # 2. Semantic Mapping
    print("\n[3] Generating semantic column mappings to canonical schema...")
    schema_mapping = generate_schema_mapping(data_columns, samples_by_col)

    # 3. Market Detection
    print("\n[4] Detecting market profile (country, currency, language)...")
    sample_records = [r.asDict() for r in df.limit(50).collect()]
    market_profile = detect_market(sample_records)

    # 4. Save metadata JSON
    profile_path = os.path.join(output_meta_dir, "profile.json")
    mapping_path = os.path.join(output_meta_dir, "mapping.json")
    market_path = os.path.join(output_meta_dir, "market.json")

    with open(profile_path, "w", encoding="utf-8") as f:
        json.dump(profile_result, f, ensure_ascii=False, indent=2)

    with open(mapping_path, "w", encoding="utf-8") as f:
        json.dump(schema_mapping, f, ensure_ascii=False, indent=2)

    with open(market_path, "w", encoding="utf-8") as f:
        json.dump(market_profile, f, ensure_ascii=False, indent=2)

    elapsed = round(time.time() - start_time, 2)
    print(f"\n[5] Profiling completed in {elapsed}s.")
    print(f"  Saved metadata to: {output_meta_dir}")
    print(f"  Detected Market: {market_profile['country']} ({market_profile['currency']}) - Conf: {market_profile['confidence']}")

    spark.stop()
    print("=" * 70)
    print("  [SUCCESS] PROFILING & MAPPING COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    run_profile_job()

