# 2_pipeline/run_pipeline.py
"""
JOBMARKET PIPELINE RUNNER — KIẾN TRÚC 3 TẦNG (TẦNG 1 • TẦNG 2 • TẦNG 3):
Thực thi toàn bộ Data Pipeline trên nền tảng Big Data Windows Native HDFS + Spark:
1. TẦNG 1 (Tang 1): Ingestion dữ liệu Raw CSV từ HDFS -> Parquet Snappy nén tối ưu.
2. Hồ sơ dữ liệu (Profiling): Phân tích Schema, thống kê null và suy luận Alias.
3. TẦNG 2 (Tang 2): Làm sạch Catalyst Native, chuẩn hóa cấp bậc/lương/địa lý/kỹ năng, Quality Gate & Quarantine.
4. TẦNG 3 (Tang 3 - SQL): 9 Bảng phân tích nghiệp vụ chuyên sâu Spark SQL -> HDFS & JSON Serving Store.
5. TẦNG 3 (Tang 3 - MLlib): Dự đoán mức lương (Random Forest) & Phân cụm thị trường (K-Means).
"""

import os
import sys
import time

# Ensure environment variables for Windows native PySpark & Hadoop
os.environ["JAVA_HOME"] = r"C:\bigdata\java"
os.environ["HADOOP_HOME"] = r"C:\bigdata\hadoop"
os.environ["SPARK_HOME"] = r"C:\bigdata\spark"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Add Hadoop & Spark bin to system PATH for winutils / hadoop.dll
hadoop_bin = r"C:\bigdata\hadoop\bin"
spark_bin = r"C:\bigdata\spark\bin"
java_bin = r"C:\bigdata\java\bin"
current_path = os.environ.get("PATH", "")
os.environ["PATH"] = f"{hadoop_bin};{spark_bin};{java_bin};{current_path}"

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "etl"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "2_pipeline"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "2_pipeline", "tang1_ingest"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "2_pipeline", "tang2_cleaning"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "2_pipeline", "tang3_analytics"))

# Import Jobs from 2_pipeline (with fallback to etl.jobs)
try:
    from tang1_ingest.tang1_job import run_tang1_job
except ImportError:
    from jobs.tang1 import run_tang1_job as run_tang1_job

try:
    from jobs.profiler_job import run_profile_job
except ImportError:
    def run_profile_job():
        print("  [INFO] Bỏ qua bước Profiler độc lập.")

try:
    from tang2_cleaning.tang2_job import run_tang2_job
except ImportError:
    from jobs.tang2 import run_tang2_job as run_tang2_job

try:
    from tang3_analytics.tang3_job import run_tang3_job
except ImportError:
    from jobs.tang3 import run_tang3_job as run_tang3_job

try:
    from tang3_analytics.ml import run_tang3_ml_job
except ImportError:
    from jobs.ml import run_ml_job as run_tang3_ml_job


def main():
    print("\n" + "#" * 80)
    print("  JOBMARKET BIG DATA — QUY TRÌNH XỬ LÝ TOÀN DIỆN PIPELINE 3 TẦNG")
    print(f"  Python: {sys.executable}")
    print("  Kiến trúc HDFS: 1 NameNode (Port 9000) + 3 DataNodes (9866, 9876, 9886)")
    print("  Mô hình dữ liệu: TẦNG 1 (Raw) -> TẦNG 2 (Sạch & Audit) -> TẦNG 3 (SQL & ML)")
    print("#" * 80 + "\n")

    overall_start = time.time()

    try:
        # BƯỚC 1: TẦNG 1 (INGESTION)
        print(">>> [BƯỚC 1/5] Thực thi TẦNG 1: Raw CSV Ingestion & Nén Parquet Snappy...")
        run_tang1_job()

        # BƯỚC 2: HỒ SƠ DỮ LIỆU (PROFILING)
        print("\n>>> [BƯỚC 2/5] Thực thi Hồ sơ Dữ liệu (Schema Profiling & Adaptive Mapping)...")
        run_profile_job()

        # BƯỚC 3: TẦNG 2 (CLEANING & QUALITY)
        print("\n>>> [BƯỚC 3/5] Thực thi TẦNG 2: Chuẩn hoá Catalyst Native & Kiểm toán Chất lượng (Quality Gate)...")
        run_tang2_job()

        # BƯỚC 4: TẦNG 3 (SPARK SQL ANALYTICS)
        print("\n>>> [BƯỚC 4/5] Thực thi TẦNG 3: 8 Bảng Phân tích Nghiệp vụ Spark SQL...")
        run_tang3_job()

        # BƯỚC 5: TẦNG 3 (MACHINE LEARNING)
        print("\n>>> [BƯỚC 5/5] Thực thi TẦNG 3: Học máy Spark MLlib (Random Forest & K-Means)...")
        run_tang3_ml_job()

        total_time = round(time.time() - overall_start, 2)
        print("\n" + "#" * 80)
        print(f"  [XUẤT SẮC] TOÀN BỘ PIPELINE 3 TẦNG ĐÃ HOÀN THÀNH THÀNH CÔNG TRONG {total_time}s!")
        print(f"  Dữ liệu Tầng 3 sẵn sàng phục vụ tại: data/tang3/ (và HDFS /tang3/jobs)")
        print("#" * 80 + "\n")

    except Exception as e:
        print(f"\n[LỖI NGHIÊM TRỌNG TRONG PIPELINE]: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def run_uploaded_hdfs_pipeline(dataset_id: str, raw_hdfs_path: str, output_root: str):
    """Run every uploaded-dataset stage from HDFS without reading the source CSV locally."""
    dataset_root = os.path.join(output_root, "datasets", dataset_id)
    meta_dir = os.path.join(dataset_root, "meta")
    tang3_json_dir = os.path.join(dataset_root, "tang3")
    hdfs_root = f"hdfs://localhost:9000/datasets/{dataset_id}"
    tang1_path = f"{hdfs_root}/tang1"
    tang2_path = f"{hdfs_root}/tang2"
    tang3_path = f"{hdfs_root}/tang3"
    quarantine_path = f"{hdfs_root}/quarantine"

    os.makedirs(meta_dir, exist_ok=True)
    os.makedirs(tang3_json_dir, exist_ok=True)
    run_tang1_job(raw_hdfs_path=raw_hdfs_path, tang1_hdfs_path=tang1_path)
    run_profile_job(tang1_hdfs_path=tang1_path, output_meta_dir=meta_dir)
    run_tang2_job(
        tang1_hdfs_path=tang1_path,
        tang2_hdfs_path=tang2_path,
        quarantine_hdfs_path=quarantine_path,
        output_meta_dir=meta_dir,
    )
    run_tang3_job(
        tang2_hdfs_path=tang2_path,
        tang3_hdfs_path=tang3_path,
        output_json_dir=tang3_json_dir,
        mirror_json_dir=None,
    )
    run_tang3_ml_job(
        tang2_hdfs_path=tang2_path,
        output_json_dir=tang3_json_dir,
        mirror_json_dir=None,
    )
    return {
        "status": "success",
        "dataset_id": dataset_id,
        "raw_hdfs_path": raw_hdfs_path,
        "tang1_hdfs_path": tang1_path,
        "tang2_hdfs_path": tang2_path,
        "tang3_hdfs_path": tang3_path,
        "serving_dir": tang3_json_dir,
    }


if __name__ == "__main__":
    main()
