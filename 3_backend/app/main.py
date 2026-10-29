# 3_backend/app/main.py
"""
FastAPI Backend API for JobMarket Intelligence:
Serves dataset metadata, profiling, adaptive schema mapping, quality metrics,
capability matrix, Tang 3 analytics (Spark SQL insights & ML models), and job searching.
Connects directly to HDFS cluster (1 NameNode + 3 DataNodes) and Tang 3 Serving Store.
"""

import os
import sys
import json
import ast
import shutil
import subprocess
import csv
import re
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List, Optional, Generator
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Project paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "etl"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "2_pipeline"))

try:
    from jobmarket_etl.sniffer import sniff_csv_format
    from jobmarket_etl.pipeline_processor import process_uploaded_dataset
    from run_pipeline import run_uploaded_hdfs_pipeline
except ImportError:
    from tang2_cleaning.jobmarket_etl.sniffer import sniff_csv_format
    from pipeline_processor import process_uploaded_dataset
    from run_pipeline import run_uploaded_hdfs_pipeline

from jobmarket_etl.normalizers.location import normalize_location
from jobmarket_etl.normalizers.salary import normalize_salary
from jobmarket_etl.normalizers.skills import normalize_skills

app = FastAPI(
    title="JobMarket Intelligence API",
    description="Big Data Job Market Analysis Platform (Hadoop/HDFS 3 DataNodes · Spark 3 Tầng · React)",
    version="1.0.0"
)

# Enable CORS for React frontend (Vite port 5173 / default port 3000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
META_DIR = os.path.join(DATA_DIR, "meta")
TANG3_DIR = os.path.join(DATA_DIR, "tang3")
TANG3_DIR = os.path.join(DATA_DIR, "tang3")
DATASETS_DIR = os.path.join(DATA_DIR, "datasets")
JOB_DB = os.path.join(DATA_DIR, "pipeline_jobs.sqlite3")
_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pipeline")
_job_lock = threading.Lock()


class SalaryPredictionRequest(BaseModel):
    level: str
    province: str
    experience_years: float = 0
    skill_count: int = 0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    connection = sqlite3.connect(JOB_DB, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute(
        """CREATE TABLE IF NOT EXISTS pipeline_jobs (
            id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL, filename TEXT NOT NULL,
            input_path TEXT NOT NULL, status TEXT NOT NULL, progress INTEGER NOT NULL,
            step TEXT, result_json TEXT, error TEXT, created_at TEXT NOT NULL,
            started_at TEXT, finished_at TEXT, cancel_requested INTEGER NOT NULL DEFAULT 0
        )"""
    )
    connection.execute(
        """CREATE TABLE IF NOT EXISTS pipeline_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
            event TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL
        )"""
    )
    connection.commit()
    return connection


def _job_event(job_id: str, event: str, payload: Dict[str, Any]) -> None:
    with _job_lock:
        db = _db()
        db.execute(
            "INSERT INTO pipeline_events(job_id,event,payload,created_at) VALUES(?,?,?,?)",
            (job_id, event, json.dumps(payload, ensure_ascii=False), _utc_now()),
        )
        db.commit()
        db.close()


def _job_row(job_id: str) -> Optional[Dict[str, Any]]:
    db = _db()
    row = db.execute("SELECT * FROM pipeline_jobs WHERE id=?", (job_id,)).fetchone()
    db.close()
    if not row:
        return None
    result = dict(row)
    result["result"] = json.loads(result.pop("result_json")) if result.get("result_json") else None
    result["cancel_requested"] = bool(result["cancel_requested"])
    return result


def _update_job(job_id: str, **fields: Any) -> None:
    if not fields:
        return
    with _job_lock:
        db = _db()
        assignments = ", ".join(f"{key}=?" for key in fields)
        db.execute(f"UPDATE pipeline_jobs SET {assignments} WHERE id=?", (*fields.values(), job_id))
        db.commit()
        db.close()


def _run_pipeline_job(job_id: str) -> None:
    row = _job_row(job_id)
    if not row:
        return
    if row["cancel_requested"]:
        _update_job(job_id, status="cancelled", finished_at=_utc_now(), step="Cancelled")
        _job_event(job_id, "cancelled", {"status": "cancelled", "progress": 0})
        return
    _update_job(job_id, status="running", started_at=_utc_now(), progress=5, step="Running pipeline")
    _job_event(job_id, "progress", {"status": "running", "progress": 5, "step": "Running pipeline"})
    try:
        _update_job(job_id, progress=15, step="Staging CSV in HDFS")
        hdfs_input = _stage_upload_in_hdfs(row["input_path"], row["dataset_id"])
        _job_event(job_id, "progress", {
            "status": "running", "progress": 15, "step": "CSV staged in HDFS", "hdfs_path": hdfs_input
        })
        result = run_uploaded_hdfs_pipeline(
            dataset_id=row["dataset_id"],
            raw_hdfs_path=f"hdfs://localhost:9000{hdfs_input}",
            output_root=DATA_DIR,
        )
        result["input_layer"] = "HDFS"
        result["hdfs_input"] = hdfs_input
        if (_job_row(job_id) or {}).get("cancel_requested"):
            _update_job(job_id, status="cancelled", finished_at=_utc_now(), step="Cancelled")
            _job_event(job_id, "cancelled", {"status": "cancelled", "progress": 100})
            return
        _update_job(job_id, status="completed", progress=100, step="Completed",
                    result_json=json.dumps(result, ensure_ascii=False), finished_at=_utc_now())
        _job_event(job_id, "completed", {"status": "completed", "progress": 100, "result": result})
    except Exception as exc:
        _update_job(job_id, status="failed", step="Failed", error=str(exc), finished_at=_utc_now())
        _job_event(job_id, "failed", {"status": "failed", "progress": 100, "error": str(exc)})


def _enqueue_job(input_path: str, dataset_id: str, filename: str) -> Dict[str, Any]:
    job_id = uuid.uuid4().hex
    now = _utc_now()
    with _job_lock:
        db = _db()
        db.execute(
            "INSERT INTO pipeline_jobs(id,dataset_id,filename,input_path,status,progress,step,created_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (job_id, dataset_id, filename, input_path, "queued", 0, "Queued", now),
        )
        db.commit()
        db.close()
    _job_event(job_id, "queued", {"status": "queued", "progress": 0, "step": "Queued"})
    _executor.submit(_run_pipeline_job, job_id)
    return _job_row(job_id)


def _hdfs_probe() -> Dict[str, Any]:
    """Probe the configured HDFS CLI; never claim healthy from static configuration."""
    command = os.environ.get("HDFS_BIN") or shutil.which("hdfs") or shutil.which("hdfs.cmd")
    if not command and os.environ.get("HADOOP_HOME"):
        candidate = os.path.join(os.environ["HADOOP_HOME"], "bin", "hdfs.cmd")
        if os.path.exists(candidate):
            command = candidate
    if not command:
        return {"reachable": False, "live_datanodes": 0, "error": "HDFS CLI not found"}
    try:
        result = subprocess.run([command, "dfsadmin", "-report"], capture_output=True,
                                text=True, timeout=8, check=False)
        output = f"{result.stdout}\n{result.stderr}"
        match = re.search(r"Live datanodes\s*\((\d+)\)", output, re.IGNORECASE)
        live = int(match.group(1)) if match else 0
        if result.returncode != 0:
            return {"reachable": False, "live_datanodes": live,
                    "error": output.strip()[-500:] or f"exit code {result.returncode}"}
        return {"reachable": True, "live_datanodes": live, "error": None}
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"reachable": False, "live_datanodes": 0, "error": str(exc)}


def _hdfs_command() -> str:
    command = os.environ.get("HDFS_BIN") or shutil.which("hdfs") or shutil.which("hdfs.cmd")
    if not command and os.environ.get("HADOOP_HOME"):
        candidate = os.path.join(os.environ["HADOOP_HOME"], "bin", "hdfs.cmd")
        if os.path.exists(candidate):
            command = candidate
    if not command:
        raise RuntimeError("HDFS CLI not found")
    return command


def _stage_upload_in_hdfs(input_path: str, dataset_id: str) -> str:
    """Make HDFS the upload boundary before the adaptive Spark-backed pipeline runs."""
    command = _hdfs_command()
    hdfs_path = f"/raw/uploads/{dataset_id}/{os.path.basename(input_path)}"
    mkdir = subprocess.run([command, "dfs", "-mkdir", "-p", f"/raw/uploads/{dataset_id}"],
                           capture_output=True, text=True, timeout=30, check=False)
    if mkdir.returncode != 0:
        raise RuntimeError(f"Unable to create HDFS upload path: {mkdir.stderr.strip()}")
    put = subprocess.run([command, "dfs", "-put", "-f", input_path, hdfs_path],
                         capture_output=True, text=True, timeout=120, check=False)
    if put.returncode != 0:
        raise RuntimeError(f"Unable to stage upload in HDFS: {put.stderr.strip()}")
    return hdfs_path


def read_json_file(file_path: str) -> Any:
    """Reads and parses a JSON file safely."""
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"Dữ liệu không tìm thấy: {os.path.basename(file_path)}")
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_dataset_paths(dataset_id: str):
    """
    Returns meta and tang3 serving directories for a specific dataset.
    Prioritizes data/tang3/ with seamless fallback to data/tang3/.
    """
    if dataset_id == "vietjobs":
        serving_dir = TANG3_DIR if os.path.exists(TANG3_DIR) else TANG3_DIR
        return META_DIR, serving_dir

    ds_meta = os.path.join(DATASETS_DIR, dataset_id, "meta")
    ds_tang3 = os.path.join(DATASETS_DIR, dataset_id, "tang3")
    ds_tang3 = os.path.join(DATASETS_DIR, dataset_id, "tang3")
    serving_dir = ds_tang3 if os.path.exists(ds_tang3) else ds_tang3

    if not os.path.exists(ds_meta):
        fallback_serving = TANG3_DIR if os.path.exists(TANG3_DIR) else TANG3_DIR
        return META_DIR, fallback_serving
    return ds_meta, serving_dir


def _load_complete_vietjobs() -> List[Dict[str, Any]]:
    """Build searchable job rows from the source when the serving sample is truncated."""
    source_path = os.path.join(PROJECT_ROOT, "VietJobs.csv")
    if not os.path.exists(source_path):
        return []

    records: List[Dict[str, Any]] = []
    with open(source_path, "r", encoding="utf-8-sig", errors="replace", newline="") as source:
        for index, row in enumerate(csv.DictReader(source)):
            location_raw = (row.get("location") or "").strip()
            location = normalize_location(location_raw)
            salary_raw = (row.get("salary") or "").strip()
            salary = normalize_salary(salary_raw)
            title = (row.get("job_title") or "Khác / Chuyên viên").strip()
            skills = normalize_skills(row.get("technical_skills") or "")
            salary_vnd = salary.get("salary_avg_vnd")
            hot_score = round(min(98.5, max(68.0, 75.0 + (index % 20))), 1)
            record = {
                "id": f"VN-{10000 + index}",
                "title": title,
                "company": (row.get("company") or "Doanh nghiệp ẩn danh").strip(),
                "location": location.get("province") or location_raw or "Toàn quốc",
                "state": location.get("province") or location_raw or "Toàn quốc",
                "salary": salary_raw or "Thỏa thuận",
                "salary_vnd": salary_vnd,
                "experience": (row.get("experience_required") or "Chưa cập nhật").strip(),
                "level": "Junior/Mid",
                "skills": skills,
                "hot_score": hot_score,
                "status": "HOT HIRING" if hot_score > 88 else "ĐANG TUYỂN",
                "recruiter": {"name": "JobMarket team", "role": "Talent acquisition", "avatar": "JM"},
                "posted_date": "",
                "description": (row.get("description") or row.get("requirements_text") or "").strip(),
            }
            education = (row.get("qualifications") or "").strip()
            try:
                parsed_education = ast.literal_eval(education)
                if isinstance(parsed_education, list):
                    education = ", ".join(str(item).strip() for item in parsed_education if str(item).strip())
            except (ValueError, SyntaxError):
                pass
            if education:
                record["education"] = education
            records.append(record)
    return records


@app.get("/")
def root():
    return {
        "message": "JobMarket Intelligence Big Data API",
        "architecture": "Kiến trúc 3 Tầng: Tầng 1 (Raw Parquet) -> Tầng 2 (Làm sạch & Quality Gate) -> Tầng 3 (SQL Analytics & MLlib)",
        "cluster": "Hadoop HDFS (1 NameNode + 3 DataNodes)",
        "engine": "Apache Spark 3.5.4 trên OpenJDK 17 LTS",
        "docs_url": "/docs"
    }


@app.get("/api/health")
def get_health():
    """Returns system and HDFS cluster health metrics."""
    hdfs = _hdfs_probe()
    cluster_status = "HEALTHY" if hdfs["reachable"] and hdfs["live_datanodes"] > 0 else "DEGRADED"
    return {
        "status": "healthy" if hdfs["reachable"] and hdfs["live_datanodes"] > 0 else "degraded",
        "pipeline_architecture": "Tầng 1 (Ingest Parquet) -> Tầng 2 (Clean & Quality) -> Tầng 3 (SQL & ML)",
        "cluster": {
            "namenode": "hdfs://localhost:9000",
            "live_datanodes": hdfs["live_datanodes"],
            "datanodes": [
                {"id": "datanode1", "port": 9866, "webui": 9864, "status": "Live" if hdfs["live_datanodes"] > 0 else "Unavailable"},
                {"id": "datanode2", "port": 9876, "webui": 9874, "status": "Live" if hdfs["live_datanodes"] > 1 else "Unavailable"},
                {"id": "datanode3", "port": 9886, "webui": 9884, "status": "Live" if hdfs["live_datanodes"] > 2 else "Unavailable"}
            ],
            "replication_factor": 3,
            "hdfs_status": f"{cluster_status} ({hdfs['live_datanodes']} live DataNodes)",
            "probe_error": hdfs["error"]
        },
        "spark": {
            "version": "3.5.4",
            "master": "local[2]",
            "default_fs": "hdfs://localhost:9000"
        }
    }


@app.get("/api/datasets")
def list_datasets():
    """Lists all available datasets."""
    source_path = os.path.join(PROJECT_ROOT, "VietJobs.csv")
    overview_path = os.path.join(TANG3_DIR, "overview.json")
    if not os.path.exists(overview_path):
        overview_path = os.path.join(TANG3_DIR, "overview.json")
    overview = read_json_file(overview_path) if os.path.exists(overview_path) else [{}]
    overview_row = overview[0] if overview else {}
    datasets = [
        {
            "id": "vietjobs",
            "name": "VietJobs Recruitment Dataset (Gốc)",
            "source": "hdfs://localhost:9000/raw/jobs/vietjobs.csv",
            "records": overview_row.get("total_jobs", 0),
            "size_mb": round(os.path.getsize(source_path) / 1_000_000, 1) if os.path.exists(source_path) else 0,
            "replication": 3,
            "status": "processed"
        }
    ]

    # Check for dynamically processed datasets
    if os.path.exists(DATASETS_DIR):
        for item in os.listdir(DATASETS_DIR):
            ds_meta = os.path.join(DATASETS_DIR, item, "meta")
            if os.path.isdir(ds_meta):
                overview_path = os.path.join(DATASETS_DIR, item, "tang3", "overview.json")
                if not os.path.exists(overview_path):
                    overview_path = os.path.join(DATASETS_DIR, item, "tang3", "overview.json")
                rec_count = 0
                market_meta = {}
                market_path = os.path.join(ds_meta, "market.json")
                if os.path.exists(market_path):
                    try:
                        with open(market_path, "r", encoding="utf-8") as f:
                            market_meta = json.load(f)
                    except Exception:
                        market_meta = {}
                if os.path.exists(overview_path):
                    try:
                        with open(overview_path, "r", encoding="utf-8") as f:
                            ov = json.load(f)
                            rec_count = ov[0].get("total_jobs", 0) if ov else 0
                    except Exception:
                        pass
                datasets.append({
                    "id": item,
                    "name": f"{market_meta.get('country_name') or 'Dataset'} · {item.replace('_', ' ').title()}",
                    "source": f"data/datasets/{item}",
                    "records": rec_count,
                    "size_mb": 0.5,
                    "replication": 1,
                    "status": "processed"
                })

    return datasets


@app.get("/api/datasets/{dataset_id}/profile")
def get_profile(dataset_id: str):
    """Returns column statistics and null rates."""
    meta_dir, _ = get_dataset_paths(dataset_id)
    return read_json_file(os.path.join(meta_dir, "profile.json"))


@app.get("/api/datasets/{dataset_id}/mapping")
def get_mapping(dataset_id: str):
    """Returns adaptive schema mapping to canonical fields."""
    meta_dir, _ = get_dataset_paths(dataset_id)
    return read_json_file(os.path.join(meta_dir, "mapping.json"))


@app.get("/api/datasets/{dataset_id}/market")
def get_market(dataset_id: str):
    """Returns detected market metadata (country, currency, language, confidence)."""
    meta_dir, _ = get_dataset_paths(dataset_id)
    return read_json_file(os.path.join(meta_dir, "market.json"))


@app.get("/api/datasets/{dataset_id}/quality")
def get_quality(dataset_id: str):
    """Returns quality score, duplicate rate, and field completeness audit."""
    meta_dir, _ = get_dataset_paths(dataset_id)
    return read_json_file(os.path.join(meta_dir, "quality.json"))


@app.get("/api/datasets/{dataset_id}/capabilities")
def get_capabilities(dataset_id: str):
    """Returns capability matrix for graceful UI degradation."""
    meta_dir, _ = get_dataset_paths(dataset_id)
    return read_json_file(os.path.join(meta_dir, "capabilities.json"))


@app.get("/api/datasets/{dataset_id}/insights/{module}")
def get_insight_module(
    dataset_id: str,
    module: str,
    year: Optional[int] = None,
    month: Optional[int] = None,
):
    """
    Returns Tang 3 analytics data for a specific dashboard module.
    """
    valid_modules = [
        "overview", "hot_jobs", "salary_by_level", "geo_distribution",
        "requirements", "top_skills", "top_companies", "year_distribution",
        "month_distribution", "hot_jobs_by_geo", "ml_metrics", "ml_clusters", "jobs"
    ]
    if module not in valid_modules:
        raise HTTPException(status_code=400, detail=f"Phân hệ không hợp lệ. Hỗ trợ: {valid_modules}")

    _, tang3_dir = get_dataset_paths(dataset_id)
    file_path = os.path.join(tang3_dir, f"{module}.json")
    if not os.path.exists(file_path):
        # Fallback to tang3 if tang3 file not found
        fallback_path = os.path.join(TANG3_DIR, f"{module}.json")
        if os.path.exists(fallback_path):
            file_path = fallback_path

    # Time-scoped dashboard requests must aggregate the same filtered jobs as
    # the job explorer. The persisted Tang 3 summaries are dataset-wide.
    if year is not None or month is not None or module == "hot_jobs_by_geo":
        jobs_path = os.path.join(tang3_dir, "jobs.json")
        if not os.path.exists(jobs_path):
            jobs_path = os.path.join(TANG3_DIR, "jobs.json")
        jobs = read_json_file(jobs_path)
        if dataset_id == "vietjobs":
            profile_path = os.path.join(META_DIR, "profile.json")
            profile = read_json_file(profile_path) if os.path.exists(profile_path) else {}
            if profile.get("total_records", 0) > len(jobs):
                jobs = _load_complete_vietjobs()
        if not isinstance(jobs, list):
            jobs = []
        if year is not None:
            jobs = [job for job in jobs if str(job.get("posted_year") or "") == str(year)]
        if month is not None:
            jobs = [job for job in jobs if str(job.get("posted_month") or "") == str(month)]

        from collections import Counter
        if module == "overview":
            salaries = sorted(float(job["salary_vnd"]) for job in jobs if job.get("salary_vnd"))
            return [{
                "total_jobs": len(jobs),
                "total_provinces": len({job.get("state") or job.get("location") for job in jobs}),
                "salary_coverage_pct": round(len(salaries) * 100 / len(jobs), 2) if jobs else 0,
                "median_salary": salaries[len(salaries) // 2] if salaries else 0,
            }]
        if module == "geo_distribution":
            counts = Counter(job.get("state") or job.get("location") or "Khác" for job in jobs)
            return [{"province": name, "region": "Khác", "job_count": count}
                    for name, count in counts.most_common()]
        if module in ("top_companies",):
            counts = Counter(job.get("company") or "Không công bố" for job in jobs)
            return [{"company": name, "job_count": count} for name, count in counts.most_common()]
        if module == "hot_jobs":
            counts = Counter(job.get("title") or "Không có tiêu đề" for job in jobs)
            return [{"clean_title": name, "job_count": count, "application_count": sum(
                int(job.get("application_count") or 0) for job in jobs if (job.get("title") or "Không có tiêu đề") == name
            )} for name, count in counts.most_common(20)]
        if module == "hot_jobs_by_geo":
            grouped = {}
            for job in jobs:
                province = job.get("state") or job.get("location") or "Khác"
                title = job.get("title") or "Không có tiêu đề"
                key = (province, title)
                row = grouped.setdefault(key, {"province": province, "job_title": title, "application_count": 0, "job_count": 0})
                row["application_count"] += int(job.get("application_count") or 0)
                row["job_count"] += 1
            return sorted(grouped.values(), key=lambda row: (row["application_count"], row["job_count"]), reverse=True)
        if module == "top_skills":
            counts = Counter(skill for job in jobs for skill in (job.get("skills") or []) if skill)
            total = len(jobs) or 1
            return [{"skill": name, "frequency": count, "prevalence_pct": round(count * 100 / total, 2)}
                    for name, count in counts.most_common(20)]
        if module == "salary_by_level":
            grouped = {}
            for job in jobs:
                salary = float(job.get("salary_vnd") or 0)
                segment = job.get("role_segment") or job.get("level") or "Vai trò phổ thông"
                if salary > 0:
                    grouped.setdefault(segment, []).append(salary)
            result = []
            for segment, values in grouped.items():
                values.sort()
                result.append({
                    "segment": segment, "level": segment, "sample_size": len(values),
                    "median_salary_vnd": values[len(values) // 2],
                    "max_salary_vnd": values[-1],
                    "p25_salary_vnd": values[len(values) // 4],
                    "p75_salary_vnd": values[(len(values) * 3) // 4],
                })
            return sorted(result, key=lambda row: row["median_salary_vnd"], reverse=True)
        if module == "requirements":
            grouped = {}
            for job in jobs:
                segment = job.get("role_segment") or job.get("level") or "Vai trò phổ thông"
                values = grouped.setdefault(segment, {"count": 0, "experience": [], "salary": []})
                values["count"] += 1
                experience = job.get("experience_years")
                if experience is not None:
                    try:
                        values["experience"].append(float(experience))
                    except (TypeError, ValueError):
                        pass
                salary = job.get("salary_vnd")
                if salary:
                    values["salary"].append(float(salary))
            return [{
                "level": segment,
                "education": None,
                "job_count": values["count"],
                "avg_min_exp_years": round(sum(values["experience"]) / len(values["experience"]), 2) if values["experience"] else 0,
                "avg_salary_vnd": round(sum(values["salary"]) / len(values["salary"]), 2) if values["salary"] else 0,
            } for segment, values in grouped.items()]
    return read_json_file(file_path)


@app.post("/api/datasets/{dataset_id}/predict-salary")
def predict_salary(dataset_id: str, request: SalaryPredictionRequest):
    """Train a small Spark MLlib model on the selected dataset and predict one row."""
    if dataset_id != "vietjobs":
        raise HTTPException(status_code=422, detail="Salary prediction is not available for this dataset.")

    rows = _load_complete_vietjobs()
    training = [
        {
            "province": row["state"],
            "level": row["level"],
            "category": "Unknown",
            "experience_years": float(re.search(r"\d+(?:\.\d+)?", row["experience"] or "").group(0))
            if re.search(r"\d+(?:\.\d+)?", row["experience"] or "") else 0.0,
            "skill_count": float(len(row["skills"] or [])),
            "label": float(row["salary_vnd"])
        }
        for row in rows
        if row.get("salary_vnd") and row["salary_vnd"] > 0
    ]
    if len(training) < 100:
        raise HTTPException(status_code=422, detail="Không đủ dữ liệu lương để huấn luyện MLlib.")

    try:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        from pyspark.sql import SparkSession
        from pyspark.ml import Pipeline
        from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
        from pyspark.ml.regression import RandomForestRegressor

        spark = SparkSession.builder.master("local[2]").appName("JobMarket-Salary-Prediction").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        frame = spark.createDataFrame(training)
        indexers = [
            StringIndexer(inputCol=field, outputCol=f"{field}_index", handleInvalid="keep")
            for field in ("province", "level", "category")
        ]
        encoder = OneHotEncoder(
            inputCols=["province_index", "level_index", "category_index"],
            outputCols=["province_vec", "level_vec", "category_vec"]
        )
        assembler = VectorAssembler(
            inputCols=["province_vec", "level_vec", "category_vec", "experience_years", "skill_count"],
            outputCol="features"
        )
        model = RandomForestRegressor(
            featuresCol="features", labelCol="label", numTrees=30, maxDepth=6, seed=42
        )
        pipeline = Pipeline(stages=indexers + [encoder, assembler, model])
        fitted = pipeline.fit(frame)
        query = spark.createDataFrame([{
            "province": request.province,
            "level": request.level,
            "category": "Unknown",
            "experience_years": float(request.experience_years),
            "skill_count": float(request.skill_count),
            "label": 0.0
        }])
        prediction = float(fitted.transform(query).select("prediction").first()["prediction"])
        spark.stop()
        return {
            "status": "completed",
            "model": "Spark MLlib RandomForestRegressor",
            "predicted_salary_vnd": round(max(0.0, prediction), 0),
            "training_samples": len(training)
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MLlib prediction failed: {exc}") from exc


@app.get("/api/datasets/{dataset_id}/jobs")
def search_jobs(
    dataset_id: str,
    q: Optional[str] = None,
    state: Optional[str] = None,
    level: Optional[str] = None,
    status: Optional[str] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    limit: int = 60
):
    """
    Search and filter jobs by keyword, state/province, seniority level, and status.
    Returns matching jobs, total matching count, and state aggregations.
    """
    _, tang3_dir = get_dataset_paths(dataset_id)
    file_path = os.path.join(tang3_dir, "jobs.json")
    if not os.path.exists(file_path):
        file_path = os.path.join(TANG3_DIR, "jobs.json")

    all_jobs = read_json_file(file_path)
    if not isinstance(all_jobs, list):
        all_jobs = []

    # jobs.json is a deliberately small UI sample in the checked-in serving
    # store. Use the complete source for vietjobs so province counts and
    # province results describe the same population.
    if dataset_id == "vietjobs":
        profile_path = os.path.join(META_DIR, "profile.json")
        profile = read_json_file(profile_path) if os.path.exists(profile_path) else {}
        expected_total = profile.get("total_records", 0)
        # Rebuild from the source because older serving jobs.json files may
        # predate education/skills fields added to the searchable contract.
        if expected_total:
            complete_jobs = _load_complete_vietjobs()
            if complete_jobs:
                all_jobs = complete_jobs

    # Calculate state counts
    from collections import Counter
    state_counter = Counter(j.get("state") or j.get("location") or "Khác" for j in all_jobs)
    state_counts = dict(state_counter.most_common())

    filtered = all_jobs
    if state and state.lower() != "all" and state.lower() != "tất cả":
        filtered = [j for j in filtered if state.lower() in (j.get("state") or j.get("location") or "").lower()]

    if q and q.strip():
        term = q.strip().lower()
        filtered = [
            j for j in filtered
            if term in j.get("title", "").lower()
            or term in j.get("company", "").lower()
            or any(term in s.lower() for s in j.get("skills", []))
            or term in j.get("location", "").lower()
        ]

    if level and level.lower() != "all" and level.lower() != "tất cả":
        filtered = [j for j in filtered if level.lower() in j.get("level", "").lower()]

    if status and status.lower() != "all":
        filtered = [j for j in filtered if status.lower() in j.get("status", "").lower()]

    if year:
        filtered = [j for j in filtered if str(j.get("posted_year") or "") == str(year)]
    if month:
        filtered = [j for j in filtered if str(j.get("posted_month") or "") == str(month)]

    return {
        "dataset_id": dataset_id,
        "total": len(filtered),
        "total_unfiltered": len(all_jobs),
        "state_counts": state_counts,
        "jobs": filtered[:limit]
    }


@app.post("/api/datasets/upload", status_code=202)
async def upload_dataset(file: UploadFile = File(...)):
    """
    Uploads a new CSV dataset, sniffs format, runs full adaptive pipeline,
    generates new Tang 3 metrics, and makes it available for immediate dashboard viewing.
    """
    filename = os.path.basename(file.filename or "")
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Chỉ hỗ trợ tệp định dạng .CSV")

    upload_dir = os.path.join(DATA_DIR, "uploads")
    os.makedirs(upload_dir, exist_ok=True)
    temp_path = os.path.join(upload_dir, f"{uuid.uuid4().hex}_{filename}")

    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    dataset_id = os.path.splitext(filename)[0].lower().replace(" ", "_")

    job = _enqueue_job(temp_path, dataset_id, filename)
    return {
        "status": "queued",
        "job_id": job["id"],
        "dataset_id": dataset_id,
        "filename": filename,
        "job": job,
        "message": f"Pipeline đã được xếp hàng xử lý cho {file.filename}."
    }


@app.get("/api/pipeline/jobs/{job_id}")
@app.get("/api/pipeline/jobs/{job_id}/status")
def get_pipeline_job(job_id: str):
    job = _job_row(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Không tìm thấy pipeline job")
    return job


@app.post("/api/pipeline/jobs/{job_id}/cancel")
@app.delete("/api/pipeline/jobs/{job_id}")
def cancel_pipeline_job(job_id: str):
    job = _job_row(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Không tìm thấy pipeline job")
    if job["status"] in ("completed", "failed", "cancelled"):
        return job
    _update_job(job_id, cancel_requested=1, status="cancelled" if job["status"] == "queued" else job["status"],
                finished_at=_utc_now() if job["status"] == "queued" else None, step="Cancelled")
    _job_event(job_id, "cancelled", {"status": "cancelled", "progress": job["progress"]})
    return _job_row(job_id)


@app.post("/api/pipeline/jobs/{job_id}/retry", status_code=202)
def retry_pipeline_job(job_id: str):
    job = _job_row(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Không tìm thấy pipeline job")
    if job["status"] not in ("failed", "cancelled"):
        raise HTTPException(status_code=409, detail="Chỉ có thể retry job đã failed hoặc cancelled")
    return {"status": "queued", "job": _enqueue_job(job["input_path"], job["dataset_id"], job["filename"])}


@app.post("/api/pipeline/jobs/{job_id}/rerun", status_code=202)
def rerun_pipeline_job(job_id: str):
    return retry_pipeline_job(job_id)


@app.get("/api/pipeline/jobs/{job_id}/events")
@app.get("/api/pipeline/jobs/{job_id}/progress")
def pipeline_events(job_id: str):
    if not _job_row(job_id):
        raise HTTPException(status_code=404, detail="Không tìm thấy pipeline job")

    def stream() -> Generator[str, None, None]:
        last_id = 0
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            db = _db()
            rows = db.execute(
                "SELECT id,event,payload FROM pipeline_events WHERE job_id=? AND id>? ORDER BY id",
                (job_id, last_id),
            ).fetchall()
            db.close()
            for row in rows:
                last_id = row["id"]
                yield f"id: {last_id}\nevent: {row['event']}\ndata: {row['payload']}\n\n"
                if row["event"] in ("completed", "failed", "cancelled"):
                    return
            time.sleep(0.25)
    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.post("/api/datasets/load-sample/{sample_id}")
def load_sample(sample_id: str):
    """
    Instantly processes pre-packaged sample dataset ('us_jobs' or 'no_salary').
    """
    valid_samples = {
        "us_jobs": os.path.join(PROJECT_ROOT, "tests", "fixtures", "us_jobs.csv"),
        "no_salary": os.path.join(PROJECT_ROOT, "tests", "fixtures", "no_salary.csv")
    }
    if sample_id not in valid_samples:
        raise HTTPException(status_code=400, detail=f"Mẫu không hợp lệ. Khả dụng: {list(valid_samples.keys())}")

    file_path = valid_samples[sample_id]
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"Không tìm thấy tệp mẫu: {os.path.basename(file_path)}")

    pipeline_res = process_uploaded_dataset(file_path, sample_id, DATA_DIR)
    return {
        "status": "success",
        "dataset_id": sample_id,
        "result": pipeline_res,
        "message": f"Đã chạy pipeline 3 Tầng thành công cho mẫu {sample_id}!"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
