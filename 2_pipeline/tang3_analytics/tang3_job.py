# 2_pipeline/tang3_analytics/tang3_job.py
"""
TẦNG 3 (TANG 3) — ANALYTICS AGGREGATION JOB (SPARK SQL):
Thực thi 7 câu truy vấn phân tích nghiệp vụ Spark SQL trên tập dữ liệu Tầng 2 (tang2_jobs):
1. Overview KPIs (tổng việc làm, doanh nghiệp, mức lương trung bình)
2. Hot Jobs (những vị trí tuyển dụng sôi động nhất)
3. Salary Analytics (phân tích lương theo cấp bậc và ngành nghề)
4. Geographic Analytics (phân bố việc làm theo tỉnh thành và vùng miền)
5. Requirements Breakdown (kinh nghiệm, bằng cấp)
6. Top Technical Skills (kỹ năng công nghệ được săn đón)
7. Top Hiring Companies (doanh nghiệp tuyển dụng hàng đầu)

Ghi kết quả Parquet xuống HDFS Tầng 3 (hdfs://localhost:9000/tang3/jobs)
và xuất bản kho JSON phục vụ trực tiếp cho Backend API & Dashboard tại:
- data/tang3/ (phục vụ chính)
- data/tang3/ (đồng bộ tương thích ngược)
"""

import sys
import os
import json
import time
from decimal import Decimal
from pyspark.sql import SparkSession

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "etl"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "2_pipeline", "tang3_analytics"))


def run_tang3_job(tang2_hdfs_path: str = "hdfs://localhost:9000/tang2/jobs",
                  tang3_hdfs_path: str = "hdfs://localhost:9000/tang3/jobs",
                  output_json_dir: str = os.path.join(PROJECT_ROOT, "data", "tang3"),
                  mirror_json_dir: str = os.path.join(PROJECT_ROOT, "data", "tang3")):
    print("=" * 70)
    print("  JOBMARKET PIPELINE — TẦNG 3: ANALYTICS AGGREGATIONS (SPARK SQL)")
    print("=" * 70)
    start_time = time.time()

    os.makedirs(output_json_dir, exist_ok=True)
    if mirror_json_dir:
        os.makedirs(mirror_json_dir, exist_ok=True)

    # Local SQL folder resolution
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_dir = os.path.join(current_dir, "sql")
    if not os.path.exists(sql_dir):
        sql_dir = os.path.join(PROJECT_ROOT, "etl", "sql")

    spark = SparkSession.builder \
        .appName("JobMarket-Tang3-Analytics") \
        .master("local[2]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read from Tang 2 with fallback to tang2
    read_path = tang2_hdfs_path
    print(f"\n[1] Đang đọc tập dữ liệu Tầng 2 từ HDFS: {read_path}")
    try:
        tang2_df = spark.read.parquet(read_path)
    except Exception:
        read_path = "hdfs://localhost:9000/tang2/jobs"
        print(f"  [Chuyển hướng] Đọc từ: {read_path}")
        tang2_df = spark.read.parquet(read_path)

    # Register both temp views so queries using either tang2_jobs or tang2_jobs will run cleanly
    tang2_df.createOrReplaceTempView("tang2_jobs")
    tang2_df.createOrReplaceTempView("tang2_jobs")
    row_count = tang2_df.count()
    print(f"  Đã đăng ký Temp View 'tang2_jobs' & 'tang2_jobs' ({row_count:,} bản ghi).")

    metrics = [
        ("overview", "01_overview.sql"),
        ("hot_jobs", "02_hot_jobs.sql"),
        ("salary_by_level", "03_salary_analytics.sql"),
        ("geo_distribution", "04_geo_analytics.sql"),
        ("requirements", "05_requirements_analytics.sql"),
        ("top_skills", "06_skills_analytics.sql"),
        ("top_companies", "07_top_companies.sql"),
        ("year_distribution", "08_year_distribution.sql")
        ,("month_distribution", "09_month_distribution.sql")
        ,("hot_jobs_by_geo", "10_hot_jobs_by_geo.sql")
    ]

    for metric_name, sql_file in metrics:
        sql_path = os.path.join(sql_dir, sql_file)
        if not os.path.exists(sql_path):
            print(f"  [WARN] Thiếu file SQL: {sql_path}, bỏ qua.")
            continue

        with open(sql_path, "r", encoding="utf-8-sig") as f:
            query = f.read()

        # Query can reference tang2_jobs or tang2_jobs
        print(f"\n[2] Thực thi Chỉ số Tầng 3: '{metric_name}' ({sql_file})...")
        metric_df = spark.sql(query)

        # Write Parquet to HDFS Tang 3
        metric_hdfs_target = f"{tang3_hdfs_path}/{metric_name}"
        try:
            metric_df.write.mode("overwrite").parquet(metric_hdfs_target)
            # Backward compatibility mirror to tang3
            metric_df.write.mode("overwrite").parquet(f"hdfs://localhost:9000/tang3/jobs/{metric_name}")
        except Exception as e:
            print(f"  [Lưu ý HDFS write]: {e}")

        # Collect and export JSON to serving store
        rows = [r.asDict() for r in metric_df.collect()]
        
        # Primary store: data/tang3/
        json_file_path = os.path.join(output_json_dir, f"{metric_name}.json")
        with open(json_file_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, default=lambda o: float(o) if isinstance(o, Decimal) else str(o), ensure_ascii=False, indent=2)

        # Mirror store: data/tang3/
        if mirror_json_dir:
            mirror_json_path = os.path.join(mirror_json_dir, f"{metric_name}.json")
            with open(mirror_json_path, "w", encoding="utf-8") as f:
                json.dump(rows, f, default=lambda o: float(o) if isinstance(o, Decimal) else str(o), ensure_ascii=False, indent=2)

        print(f"  [OK] Đã xuất {len(rows)} bản ghi -> HDFS {metric_hdfs_target} & JSON ({json_file_path})")

    # Export the interactive job list from Tang 2, not from the upload buffer.
    # This keeps the jobs screen backed by the same HDFS-derived dataset as the
    # aggregate analytics above.
    job_rows = []
    for row in tang2_df.select(
        "_row_id", "job_title", "company", "province", "salary_raw",
        "salary_avg_vnd", "experience_group", "level", "role_segment", "skills", "education",
        "application_count", "posted_date_raw", "posted_year", "posted_month"
    ).collect():
        item = row.asDict()
        job_rows.append({
            "id": str(item.get("_row_id")),
            "title": item.get("job_title") or "Không có tiêu đề",
            "company": item.get("company") or "N/A",
            "location": item.get("province") or "Toàn quốc",
            "state": item.get("province") or "Toàn quốc",
            "salary": item.get("salary_raw") or "Thỏa thuận",
            "salary_vnd": item.get("salary_avg_vnd"),
            "experience": item.get("experience_group") or "Không yêu cầu",
            "education": item.get("education"),
            "level": item.get("level") or "Junior/Mid",
            "role_segment": item.get("role_segment") or item.get("level") or "Vai trò phổ thông",
            "skills": item.get("skills") or [],
            "application_count": item.get("application_count") or 0,
            "posted_date": item.get("posted_date_raw") or "",
            "posted_year": item.get("posted_year"),
            "posted_month": item.get("posted_month"),
            "status": "Đang tuyển",
        })

    jobs_path = os.path.join(output_json_dir, "jobs.json")
    with open(jobs_path, "w", encoding="utf-8") as f:
        json.dump(job_rows, f, default=lambda o: float(o) if isinstance(o, Decimal) else str(o),
                  ensure_ascii=False, indent=2)
    if mirror_json_dir:
        with open(os.path.join(mirror_json_dir, "jobs.json"), "w", encoding="utf-8") as f:
            json.dump(job_rows, f, default=lambda o: float(o) if isinstance(o, Decimal) else str(o),
                      ensure_ascii=False, indent=2)
    print(f"  [OK] Đã xuất {len(job_rows)} tin tuyển dụng -> {jobs_path}")

    elapsed = round(time.time() - start_time, 2)
    print("\n" + "=" * 70)
    print(f"  [SUCCESS] TẦNG 3 ANALYTICS HOÀN TẤT TRONG {elapsed} GIÂY")
    print("=" * 70)

    spark.stop()


# Backward compatibility alias
run_tang3_job = run_tang3_job

if __name__ == "__main__":
    run_tang3_job()
