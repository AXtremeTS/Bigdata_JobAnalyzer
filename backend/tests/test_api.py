# backend/tests/test_api.py
"""
Backend API integration tests using FastAPI TestClient.
"""

import sys
import os
import pytest
from fastapi.testclient import TestClient

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "backend"))

from app.main import app

client = TestClient(app)


def test_root_endpoint():
    res = client.get("/")
    assert res.status_code == 200
    assert "Hadoop HDFS" in res.json()["cluster"]


def test_health_endpoint():
    res = client.get("/api/health")
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "healthy"
    assert data["cluster"]["live_datanodes"] == 3


def test_datasets_list():
    res = client.get("/api/datasets")
    assert res.status_code == 200
    datasets = res.json()
    assert len(datasets) >= 1
    assert datasets[0]["id"] == "vietjobs"
    assert datasets[0]["records"] == 48092


def test_market_endpoint():
    res = client.get("/api/datasets/vietjobs/market")
    assert res.status_code == 200
    market = res.json()
    assert market["country"] == "VN"
    assert market["currency"] == "VND"


def test_quality_endpoint():
    res = client.get("/api/datasets/vietjobs/quality")
    assert res.status_code == 200
    quality = res.json()
    assert quality["grade"] in ("A", "B")
    assert quality["total_records"] == 48092


def test_capabilities_endpoint():
    res = client.get("/api/datasets/vietjobs/capabilities")
    assert res.status_code == 200
    caps = res.json()
    assert caps["modules"]["overview"]["enabled"] is True
    assert caps["modules"]["hot_jobs"]["enabled"] is True


def test_insight_hot_jobs():
    res = client.get("/api/datasets/vietjobs/insights/hot_jobs")
    assert res.status_code == 200
    hot_jobs = res.json()
    assert len(hot_jobs) > 0
    assert "job_title" in hot_jobs[0]


def test_insight_ml_metrics():
    res = client.get("/api/datasets/vietjobs/insights/ml_metrics")
    assert res.status_code == 200
    ml = res.json()
    assert ml["model_type"] == "RandomForestRegressor"
    assert ml["sample_count"] > 10000
