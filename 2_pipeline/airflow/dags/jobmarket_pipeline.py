# 2_pipeline/airflow/dags/jobmarket_pipeline.py
"""
Apache Airflow DAG for JobMarket Intelligence:
Orchestrates the end-to-end 3-tier Big Data pipeline (Tầng 1 -> Tầng 2 -> Tầng 3)
on Windows Native HDFS (1 NameNode + 3 DataNodes) & Apache Spark 3.5.4:
1. Validate & Ingest Raw -> HDFS
2. Tầng 1 (Tang 1): Ingestion & Parquet Snappy
3. Profiling & Adaptive Schema Mapping
4. Tầng 2 (Tang 2): Làm sạch Catalyst Native, Chuẩn hoá & Quality Gate
5. Tầng 3 (Tang 3): 7 Bảng Spark SQL Analytics & MLlib (Random Forest, K-Means)
6. Publish to Serving Store & Completion Notification
"""

from datetime import datetime, timedelta
import os
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup

# Default project paths
PROJECT_ROOT = os.getenv("JOBMARKET_HOME", r"C:\Users\axtre\OneDrive\Desktop\visual studio\Teamproject_Hadoop")
PYTHON_BIN = os.getenv("PYTHON_BIN", "python")

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 10, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}


def evaluate_quality_gate(**context):
    """
    Quality Gate Branch:
    Inspects quality.json produced by Tang 2 stage.
    If quality score >= 60, proceeds to Tang 3 analytics.
    Otherwise, diverts to quarantine_alert.
    """
    quality_path = os.path.join(PROJECT_ROOT, "data", "meta", "quality.json")
    if os.path.exists(quality_path):
        try:
            with open(quality_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                score = data.get("overall_score", 0.0)
                if score >= 60.0:
                    return "tang3_analytics.tang3_sql_analytics"
        except Exception:
            pass
    return "quarantine_alert"


def check_ml_eligibility(**context):
    """
    ML Gate:
    Checks if capabilities.json has sufficient salary records to train MLlib models.
    """
    caps_path = os.path.join(PROJECT_ROOT, "data", "meta", "capabilities.json")
    if os.path.exists(caps_path):
        try:
            with open(caps_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if data.get("modules", {}).get("ml_salary", {}).get("enabled", False):
                    return "tang3_spark_mllib"
        except Exception:
            pass
    return "skip_mllib"


with DAG(
    dag_id="jobmarket_intelligence_pipeline",
    default_args=default_args,
    description="Quy trình Big Data 3 Tầng: Tầng 1 (Raw) -> Tầng 2 (Sạch) -> Tầng 3 (SQL & ML) trên HDFS 3 DataNodes & Spark",
    schedule_interval=None,  # Triggered on-demand via API or UI
    catchup=False,
    tags=["bigdata", "hdfs", "spark", "tang1_tang2_tang3", "vietjobs"]
) as dag:

    # Task 1: Validate Input
    validate_input = BashOperator(
        task_id="validate_input",
        bash_command=f'{PYTHON_BIN} -c "import os; assert os.path.exists(r\'{PROJECT_ROOT}\\VietJobs.csv\'), \'Missing VietJobs.csv\'"'
    )

    # Task 2: Ingest Raw to HDFS
    ingest_raw_hdfs = BashOperator(
        task_id="ingest_raw_hdfs",
        bash_command=f'powershell -NoProfile -Command "Set-Location \'C:\\bigdata\'; & \'C:\\bigdata\\hadoop\\bin\\hdfs.cmd\' dfs -test -e /raw/jobs/vietjobs.csv; if ($LASTEXITCODE -ne 0) {{ & \'C:\\bigdata\\hadoop\\bin\\hdfs.cmd\' dfs -put \'{PROJECT_ROOT}\\VietJobs.csv\' /raw/jobs/vietjobs.csv }}"'
    )

    # Task 3: Tầng 1 Ingestion
    tang1_ingest = BashOperator(
        task_id="tang1_ingest",
        bash_command=f'{PYTHON_BIN} "{PROJECT_ROOT}\\2_pipeline\\tang1_ingest\\tang1_job.py"'
    )

    # Task 4: Profile & Adaptive Schema Mapping
    profile_and_map = BashOperator(
        task_id="profile_and_map",
        bash_command=f'{PYTHON_BIN} -c "import sys; sys.path.insert(0, r\'{PROJECT_ROOT}\\etl\'); from jobs.profiler_job import run_profile_job; run_profile_job()"'
    )

    # Task 5: Tầng 2 Cleaning & Quality Gate
    tang2_cleaning = BashOperator(
        task_id="tang2_cleaning",
        bash_command=f'{PYTHON_BIN} "{PROJECT_ROOT}\\2_pipeline\\tang2_cleaning\\tang2_job.py"'
    )

    # Task 6: Quality Gate Branch
    quality_gate = BranchPythonOperator(
        task_id="quality_gate",
        python_callable=evaluate_quality_gate
    )

    quarantine_alert = BashOperator(
        task_id="quarantine_alert",
        bash_command='echo "[CẢNH BÁO] Bản ghi lỗi được cách ly tại HDFS /quarantine/jobs"'
    )

    # TaskGroup: Tầng 3 Spark SQL Analytics
    with TaskGroup("tang3_analytics", tooltip="Thực thi 7 Bảng Phân tích Spark SQL Tầng 3") as tang3_group:
        tang3_sql_analytics = BashOperator(
            task_id="tang3_sql_analytics",
            bash_command=f'{PYTHON_BIN} "{PROJECT_ROOT}\\2_pipeline\\tang3_analytics\\tang3_job.py"'
        )

    # Task 7: ML Branch Decision
    ml_decision = BranchPythonOperator(
        task_id="ml_eligibility_check",
        python_callable=check_ml_eligibility
    )

    tang3_spark_mllib = BashOperator(
        task_id="tang3_spark_mllib",
        bash_command=f'{PYTHON_BIN} "{PROJECT_ROOT}\\2_pipeline\\tang3_analytics\\ml.py"'
    )

    skip_mllib = BashOperator(
        task_id="skip_mllib",
        bash_command='echo "[THÔNG TIN] Tập dữ liệu lương không đủ kích thước để huấn luyện mô hình MLlib"'
    )

    # Task 8: Notify Done
    notify_done = BashOperator(
        task_id="notify_pipeline_completed",
        trigger_rule="none_failed_min_one_success",
        bash_command='echo "=========================================" && echo "  PIPELINE 3 TẦNG HOÀN THÀNH THÀNH CÔNG" && echo "========================================="'
    )

    # Pipeline Dependencies
    validate_input >> ingest_raw_hdfs >> tang1_ingest >> profile_and_map >> tang2_cleaning >> quality_gate
    quality_gate >> [tang3_group, quarantine_alert]
    tang3_group >> ml_decision
    ml_decision >> [tang3_spark_mllib, skip_mllib]
    [tang3_spark_mllib, skip_mllib, quarantine_alert] >> notify_done
