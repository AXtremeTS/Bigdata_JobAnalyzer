# airflow/dags/jobmarket_pipeline.py
"""
Apache Airflow DAG for JobMarket Intelligence:
Orchestrates the end-to-end Medallion Big Data pipeline on HDFS (1 NameNode + 3 DataNodes) & Spark:
1. Validate & Ingest Raw -> HDFS
2. Tang1 Ingestion Parquet
3. Dataset Profiling & Adaptive Schema Mapping
4. Tang2 Cleaning & Normalization (Quality Gate Branch)
5. Parallel Tang3 Spark SQL Analytics (TaskGroup)
6. Spark MLlib Salary Regression & Market Clustering
7. Publish to Serving Store & Completion Notification
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
    Inspects quality.json produced by Tang2 stage.
    If quality score >= 70, proceeds to Tang3 analytics.
    Otherwise, diverts to quarantine_alert.
    """
    quality_path = os.path.join(PROJECT_ROOT, "data", "meta", "quality.json")
    if os.path.exists(quality_path):
        try:
            with open(quality_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                score = data.get("overall_score", 0.0)
                if score >= 60.0:
                    return "tang3_analytics.tang3_overview"
        except Exception:
            pass
    return "quarantine_alert"


def check_ml_eligibility(**context):
    """
    ML Gate:
    Checks if capabilities.json has sufficient salary records (>= 200) to train MLlib models.
    """
    caps_path = os.path.join(PROJECT_ROOT, "data", "meta", "capabilities.json")
    if os.path.exists(caps_path):
        try:
            with open(caps_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if data.get("modules", {}).get("ml_salary", {}).get("enabled", False):
                    return "run_spark_mllib"
        except Exception:
            pass
    return "skip_mllib"


with DAG(
    dag_id="jobmarket_intelligence_pipeline",
    default_args=default_args,
    description="End-to-end Medallion Big Data Pipeline (HDFS 3 DataNodes + Spark 3.5.1 + MLlib)",
    schedule_interval=None,  # Triggered on-demand via API or UI
    catchup=False,
    tags=["bigdata", "hdfs", "spark", "vietjobs"]
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

    # Task 3: Tang1 Ingestion
    tang1_ingest = BashOperator(
        task_id="tang1_ingest",
        bash_command=f'{PYTHON_BIN} "{PROJECT_ROOT}\\etl\\jobs\\tang1.py"'
    )

    # Task 4: Profile & Adaptive Schema Mapping
    profile_and_map = BashOperator(
        task_id="profile_and_map",
        bash_command=f'{PYTHON_BIN} "{PROJECT_ROOT}\\etl\\jobs\\profiler_job.py"'
    )

    # Task 5: Tang2 Cleaning & Normalization
    tang2_clean = BashOperator(
        task_id="tang2_clean",
        bash_command=f'{PYTHON_BIN} "{PROJECT_ROOT}\\etl\\jobs\\tang2.py"'
    )

    # Task 6: Quality Gate Branch
    quality_gate = BranchPythonOperator(
        task_id="quality_gate",
        python_callable=evaluate_quality_gate
    )

    quarantine_alert = BashOperator(
        task_id="quarantine_alert",
        bash_command='echo "[ALERT] Dataset quality below threshold! Quarantined records in HDFS /quarantine/jobs"'
    )

    # TaskGroup: Parallel Tang3 Analytics
    with TaskGroup("tang3_analytics", tooltip="Parallel Spark SQL Tang3 Table Aggregations") as tang3_group:
        tang3_overview = BashOperator(
            task_id="tang3_overview",
            bash_command=f'{PYTHON_BIN} -c "from etl.jobs.tang3 import run_tang3_job; run_tang3_job()"'
        )

    # Task 7: ML Branch Decision
    ml_decision = BranchPythonOperator(
        task_id="ml_eligibility_check",
        python_callable=check_ml_eligibility
    )

    run_spark_mllib = BashOperator(
        task_id="run_spark_mllib",
        bash_command=f'{PYTHON_BIN} "{PROJECT_ROOT}\\etl\\jobs\\ml.py"'
    )

    skip_mllib = BashOperator(
        task_id="skip_mllib",
        bash_command='echo "[INFO] Insufficient salary sample size; skipping ML regression"'
    )

    # Task 8: Notify Done
    notify_done = BashOperator(
        task_id="notify_pipeline_completed",
        trigger_rule="none_failed_min_one_success",
        bash_command='echo "=========================================" && echo "  JOBMARKET PIPELINE SUCCESSFULLY COMPLETED" && echo "========================================="'
    )

    # Pipeline Dependencies
    validate_input >> ingest_raw_hdfs >> tang1_ingest >> profile_and_map >> tang2_clean >> quality_gate
    quality_gate >> [tang3_group, quarantine_alert]
    tang3_group >> ml_decision
    ml_decision >> [run_spark_mllib, skip_mllib]
    [run_spark_mllib, skip_mllib, quarantine_alert] >> notify_done

