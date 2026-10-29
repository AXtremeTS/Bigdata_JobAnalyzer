# etl/jobs/tang2.py
"""
Tang2 Cleaning & Normalization Job (Catalyst Optimized):
Reads Tang1 dataset from HDFS, applies schema transformations using high-performance
Spark Catalyst native expressions (Tungsten engine), audits quality, isolates corrupt
records into quarantine, and writes partitioned Parquet to HDFS Tang2 layer.
Emits quality.json and capabilities.json.
"""

import sys
import os
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, count, round as spark_round,
    regexp_extract, regexp_replace, split, trim, size, array_distinct
)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "etl"))

from jobmarket_etl.quality import evaluate_quality_metrics
from jobmarket_etl.capabilities import build_capabilities


def run_tang2_job(tang1_hdfs_path: str = "hdfs://localhost:9000/tang1/jobs",
                   tang2_hdfs_path: str = "hdfs://localhost:9000/tang2/jobs",
                   quarantine_hdfs_path: str = "hdfs://localhost:9000/quarantine/jobs",
                   output_meta_dir: str = os.path.join(PROJECT_ROOT, "data", "meta")):
    print("=" * 70)
    print("  JOBMARKET PIPELINE — TANG2 CLEANING & NORMALIZATION (SPARK)")
    print("=" * 70)
    start_time = time.time()

    os.makedirs(output_meta_dir, exist_ok=True)

    spark = SparkSession.builder \
        .appName("JobMarket-Tang2-Normalization") \
        .master("local[2]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"\n[1] Reading Tang1 data from HDFS: {tang1_hdfs_path}")
    tang1_df = spark.read.parquet(tang1_hdfs_path)
    total_tang1 = tang1_df.count()
    print(f"  Loaded {total_tang1:,} Tang1 records.")

    # -------------------------------------------------------------
    # 2. CATALYST NATIVE TRANSFORMATIONS
    # -------------------------------------------------------------
    print("\n[2] Applying canonical normalizers via Spark Catalyst...")
    cols = set(tang1_df.columns)

    # Resolve column names
    title_col = col("job_title") if "job_title" in cols else lit(None)
    loc_col = col("location") if "location" in cols else lit(None)
    sal_col = col("salary") if "salary" in cols else lit(None)
    sal_min_col = col("salary_min") if "salary_min" in cols else lit(None)
    sal_max_col = col("salary_max") if "salary_max" in cols else lit(None)
    exp_col = col("experience_required") if "experience_required" in cols else lit(None)
    edu_col = col("qualifications") if "qualifications" in cols else lit(None)
    tech_col = col("technical_skills") if "technical_skills" in cols else lit(None)
    desc_col = col("description") if "description" in cols else lit(None)
    req_col = col("requirements_text") if "requirements_text" in cols else lit(None)
    cat_col = col("category") if "category" in cols else lit("Khác")
    contract_col = col("contract_type") if "contract_type" in cols else lit("Toàn thời gian")

    # Salary normalization
    # Parse numbers from min/max or salary string
    is_negotiable = when(sal_col.rlike("(?i)thoả thuận|thoa thuan|negotiable|thương lượng|cạnh tranh"), True).otherwise(False)
    
    # Extract min / max in millions if not already in integer format
    sal_min_val = when(sal_min_col.isNotNull() & (sal_min_col > 0),
                       when(sal_min_col < 1000, sal_min_col * 1000000).otherwise(sal_min_col)) \
                  .when(sal_col.rlike(r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)"),
                        regexp_extract(sal_col, r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)", 1).cast("double") * 1000000) \
                  .otherwise(None).cast("long")

    sal_max_val = when(sal_max_col.isNotNull() & (sal_max_col > 0),
                       when(sal_max_col < 1000, sal_max_col * 1000000).otherwise(sal_max_col)) \
                  .when(sal_col.rlike(r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)"),
                        regexp_extract(sal_col, r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)", 2).cast("double") * 1000000) \
                  .otherwise(sal_min_val).cast("long")

    sal_avg_val = when(sal_min_val.isNotNull() & sal_max_val.isNotNull(),
                       ((sal_min_val + sal_max_val) / 2).cast("long")) \
                  .when(sal_min_val.isNotNull(), sal_min_val) \
                  .otherwise(None)

    sal_avg_usd = when(sal_avg_val.isNotNull(), spark_round(sal_avg_val / 25450.0, 2)).otherwise(None)

    is_outlier = when(sal_avg_val > 500000000, True) \
                 .when(sal_avg_val < 1000000, True) \
                 .otherwise(False)

    # Location normalization (Provinces & Regions)
    province = when(loc_col.rlike("(?i)hồ chí minh|tphcm|hcm|sài gòn"), "Hồ Chí Minh") \
               .when(loc_col.rlike("(?i)hà nội|hanoi|hn"), "Hà Nội") \
               .when(loc_col.rlike("(?i)đà nẵng|da nang"), "Đà Nẵng") \
               .when(loc_col.rlike("(?i)bình dương|binh duong"), "Bình Dương") \
               .when(loc_col.rlike("(?i)đồng nai|dong nai"), "Đồng Nai") \
               .when(loc_col.rlike("(?i)vũng tàu|bà rịa"), "Bà Rịa - Vũng Tàu") \
               .when(loc_col.rlike("(?i)hải phòng|hai phong"), "Hải Phòng") \
               .when(loc_col.rlike("(?i)cần thơ|can tho"), "Cần Thơ") \
               .when(loc_col.rlike("(?i)vĩnh long"), "Vĩnh Long") \
               .when(loc_col.rlike("(?i)nghệ an"), "Nghệ An") \
               .when(loc_col.rlike("(?i)thanh hóa"), "Thanh Hóa") \
               .when(loc_col.rlike("(?i)khánh hòa|nha trang"), "Khánh Hòa") \
               .when(loc_col.rlike("(?i)lâm đồng|đà lạt"), "Lâm Đồng") \
               .when(loc_col.rlike("(?i)bắc ninh"), "Bắc Ninh") \
               .when(loc_col.rlike("(?i)quảng ninh"), "Quảng Ninh") \
               .otherwise(trim(loc_col))

    region = when(province.isin("Hà Nội", "Hải Phòng", "Bắc Ninh", "Quảng Ninh", "Hải Dương", "Hưng Yên", "Nghệ An", "Thanh Hóa"), "Bắc") \
             .when(province.isin("Đà Nẵng", "Thừa Thiên Huế", "Quảng Nam", "Khánh Hòa", "Bình Định", "Lâm Đồng"), "Trung") \
             .otherwise("Nam")

    # Seniority Level normalization
    level = when(title_col.rlike("(?i)giám đốc|director|head of|phó giám đốc|vp|c-level"), "Director") \
            .when(title_col.rlike("(?i)trưởng phòng|manager|trưởng nhóm|lead|quản lý|chủ nhiệm|architect"), "Manager/Lead") \
            .when(title_col.rlike("(?i)senior|chuyên viên cao cấp|chuyên gia|sr\\.?"), "Senior") \
            .when(title_col.rlike("(?i)thực tập|intern|học việc|sinh viên|trainee"), "Intern") \
            .when(title_col.rlike("(?i)fresher|mới tốt nghiệp|entry"), "Fresher") \
            .otherwise("Junior/Mid")

    # Education normalization
    education = when(edu_col.rlike("(?i)tiến sĩ|phd|doctorate"), "Tiến sĩ") \
                .when(edu_col.rlike("(?i)thạc sĩ|master|cao học"), "Thạc sĩ") \
                .when(edu_col.rlike("(?i)đại học|cử nhân|kỹ sư|bachelor|university"), "Đại học") \
                .when(edu_col.rlike("(?i)cao đẳng|college"), "Cao đẳng") \
                .when(edu_col.rlike("(?i)trung cấp|nghề|vocational"), "Trung cấp") \
                .when(edu_col.rlike("(?i)thpt|cấp 3|high school"), "THPT") \
                .otherwise("Không yêu cầu")

    # Experience normalization
    exp_years_raw = regexp_extract(exp_col, r"(\d+(?:\.\d+)?)", 1)
    exp_min_years = when(exp_col.rlike("(?i)không yêu cầu|chưa có kinh nghiệm|fresher"), 0.0) \
                    .when(exp_years_raw != "", exp_years_raw.cast("double")) \
                    .otherwise(None)

    # Technical Skills array
    # Clean brackets and split by comma
    cleaned_skills_str = regexp_replace(tech_col, r"[\[\]'\"]", "")
    skills_array = when(tech_col.isNotNull() & (trim(tech_col) != "") & (tech_col != "[]"),
                        array_distinct(split(trim(cleaned_skills_str), r",\s*"))) \
                   .otherwise(lit(None))

    # Assemble canonical DataFrame
    canonical_df = tang1_df.select(
        col("_row_id"),
        col("_source_file"),
        col("_ingest_ts"),
        title_col.alias("job_title"),
        coalesce(col("company") if "company" in cols else lit("N/A"), lit("N/A")).alias("company"),
        desc_col.alias("description"),
        req_col.alias("requirements"),
        cat_col.alias("category"),
        contract_col.alias("contract_type"),
        loc_col.alias("location_raw"),
        province.alias("province"),
        region.alias("region"),
        lit("Việt Nam").alias("country"),
        sal_col.alias("salary_raw"),
        sal_min_val.alias("salary_min_vnd"),
        sal_max_val.alias("salary_max_vnd"),
        sal_avg_val.alias("salary_avg_vnd"),
        sal_avg_usd.alias("salary_avg_usd"),
        lit("VND").alias("salary_currency"),
        lit("month").alias("salary_period"),
        is_negotiable.alias("is_salary_negotiable"),
        is_outlier.alias("is_salary_outlier"),
        exp_min_years.alias("exp_min_years"),
        education.alias("education"),
        level.alias("level"),
        skills_array.alias("technical_skills")
    )

    # -------------------------------------------------------------
    # 3. QUALITY GATE & QUARANTINE
    # -------------------------------------------------------------
    print("\n[3] Auditing data quality and isolating corrupt records...")
    corrupt_cond = col("job_title").isNull() | (trim(col("job_title")) == "")

    quarantine_df = canonical_df.filter(corrupt_cond).withColumn("quarantine_reason", lit("Missing job_title"))
    valid_df = canonical_df.filter(~corrupt_cond)

    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count()

    print(f"  Valid records: {valid_count:,} | Quarantine records: {quarantine_count:,}")

    if quarantine_count > 0:
        quarantine_df.write.mode("overwrite").parquet(quarantine_hdfs_path)

    # -------------------------------------------------------------
    # 4. WRITE PARTITIONED TANG2 PARQUET TO HDFS
    # -------------------------------------------------------------
    print(f"\n[4] Writing cleaned Tang2 Parquet partitioned by 'country':")
    print(f"  Target: {tang2_hdfs_path}")
    valid_df.write \
        .mode("overwrite") \
        .partitionBy("country") \
        .parquet(tang2_hdfs_path)

    # -------------------------------------------------------------
    # 5. DATA QUALITY REPORT & CAPABILITY MATRIX
    # -------------------------------------------------------------
    print("\n[5] Computing Data Quality Report and Capability Matrix...")
    stats = valid_df.select([
        count(when(col("job_title").isNotNull() & (col("job_title") != ""), 1)).alias("job_title_valid"),
        count(when(col("company").isNotNull() & (col("company") != "N/A"), 1)).alias("company_valid"),
        count(when(col("province").isNotNull() & (col("province") != ""), 1)).alias("province_valid"),
        count(when(col("salary_avg_vnd").isNotNull() & (~col("is_salary_negotiable")), 1)).alias("salary_valid"),
        count(when(col("level").isNotNull() & (col("level") != "Not Specified"), 1)).alias("level_valid"),
        count(when(col("education").isNotNull() & (col("education") != "Không yêu cầu"), 1)).alias("education_valid"),
        count(when(col("exp_min_years").isNotNull(), 1)).alias("exp_valid"),
        count(when(col("technical_skills").isNotNull() & (size(col("technical_skills")) > 0), 1)).alias("skills_valid")
    ]).collect()[0]

    valid_counts = {
        "job_title": stats["job_title_valid"],
        "company": stats["company_valid"],
        "province": stats["province_valid"],
        "salary_monthly_vnd": stats["salary_valid"],
        "level": stats["level_valid"],
        "education": stats["education_valid"],
        "exp_min_years": stats["exp_valid"],
        "technical_skills": stats["skills_valid"]
    }

    null_counts = {k: valid_count - v for k, v in valid_counts.items()}

    quality_rep = evaluate_quality_metrics(
        total_records=valid_count,
        null_counts=null_counts,
        duplicate_count=0,
        quarantine_count=quarantine_count,
        quarantine_reasons={"Missing job_title": quarantine_count} if quarantine_count > 0 else {}
    )

    capabilities_rep = build_capabilities(
        total_records=valid_count,
        valid_counts=valid_counts
    )

    with open(os.path.join(output_meta_dir, "quality.json"), "w", encoding="utf-8") as f:
        json.dump(quality_rep, f, ensure_ascii=False, indent=2)

    with open(os.path.join(output_meta_dir, "capabilities.json"), "w", encoding="utf-8") as f:
        json.dump(capabilities_rep, f, ensure_ascii=False, indent=2)

    elapsed = round(time.time() - start_time, 2)
    print(f"\n[6] Tang2 Stage completed in {elapsed}s.")
    print(f"  Quality Grade: {quality_rep['grade']} (Score: {quality_rep['overall_score']} / 100)")
    print(f"  Active Capabilities: {[m for m, v in capabilities_rep['modules'].items() if v['enabled']]}")

    spark.stop()
    print("=" * 70)
    print("  [SUCCESS] TANG2 STAGE COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    run_tang2_job()

