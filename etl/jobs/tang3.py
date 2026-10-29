# etl/jobs/tang3.py
"""
Tang3 Aggregation Job:
Executes Spark SQL queries against Tang2 dataset, calculates business metrics:
1. Overview KPIs
2. Hot Jobs
3. Salary Analytics by Role & Level
4. Geographic Analytics (Province & Region)
5. Requirements Breakdown
6. Top Technical Skills
7. Top Hiring Companies
Writes Parquet to HDFS Tang3 layer and exports JSON to data/tang3/ for serving.
"""

import sys
import os
import json
import time
from pyspark.sql import SparkSession

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "etl"))


def run_tang3_job(tang2_hdfs_path: str = "hdfs://localhost:9000/tang2/jobs",
                 tang3_hdfs_path: str = "hdfs://localhost:9000/tang3/jobs",
                 output_json_dir: str = os.path.join(PROJECT_ROOT, "data", "tang3")):
    print("=" * 70)
    print("  JOBMARKET PIPELINE — TANG3 AGGREGATIONS (SPARK SQL)")
    print("=" * 70)
    start_time = time.time()

    os.makedirs(output_json_dir, exist_ok=True)
    sql_dir = os.path.join(PROJECT_ROOT, "etl", "sql")

    spark = SparkSession.builder \
        .appName("JobMarket-Tang3-Analytics") \
        .master("local[2]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"\n[1] Reading Tang2 dataset from HDFS: {tang2_hdfs_path}")
    tang2_df = spark.read.parquet(tang2_hdfs_path)
    tang2_df.createOrReplaceTempView("tang2_jobs")
    print(f"  Registered 'tang2_jobs' temp view with {tang2_df.count():,} rows.")

    metrics = [
        ("overview", "01_overview.sql"),
        ("hot_jobs", "02_hot_jobs.sql"),
        ("salary_by_level", "03_salary_analytics.sql"),
        ("geo_distribution", "04_geo_analytics.sql"),
        ("requirements", "05_requirements_analytics.sql"),
        ("top_skills", "06_skills_analytics.sql"),
        ("top_companies", "07_top_companies.sql")
    ]

    for metric_name, sql_file in metrics:
        sql_path = os.path.join(sql_dir, sql_file)
        if not os.path.exists(sql_path):
            print(f"  [WARN] Missing SQL file: {sql_path}, skipping.")
            continue

        with open(sql_path, "r", encoding="utf-8") as f:
            query = f.read()

        print(f"\n[2] Executing Metric: '{metric_name}' ({sql_file})...")
        metric_df = spark.sql(query)

        # Write Parquet to HDFS
        metric_hdfs_target = f"{tang3_hdfs_path}/{metric_name}"
        metric_df.write.mode("overwrite").parquet(metric_hdfs_target)

        from decimal import Decimal
        # Export JSON to local serving store
        rows = [r.asDict() for r in metric_df.collect()]
        json_file_path = os.path.join(output_json_dir, f"{metric_name}.json")
        with open(json_file_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, default=lambda o: float(o) if isinstance(o, Decimal) else str(o), ensure_ascii=False, indent=2)

        print(f"  [OK] Saved {len(rows)} records -> HDFS {metric_hdfs_target} & local JSON")

    elapsed = round(time.time() - start_time, 2)
    print("\n" + "=" * 70)
    print(f"  [SUCCESS] TANG3 ANALYTICS COMPLETED in {elapsed}s")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    run_tang3_job()

