# tests/test_adaptive_pipeline.py
"""
Test Suite for Adaptive Schema and Multi-Dataset Capabilities:
Validates that JobMarket Intelligence dynamically adapts to different dataset schemas
(e.g., US/UK international jobs vs Vietnamese no-salary jobs) with graceful degradation.
Covers Checkpoints CP-31 and CP-32.
"""

import os
import sys
import csv
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "etl"))

from jobmarket_etl.sniffer import sniff_csv_format
from jobmarket_etl.mapper import generate_schema_mapping
from jobmarket_etl.market_detector import detect_market
from jobmarket_etl.capabilities import build_capabilities


def test_us_jobs_adaptive_profiling():
    """CP-31: Validates US/UK international dataset profiling and market detection."""
    us_csv = os.path.join(PROJECT_ROOT, "tests", "fixtures", "us_jobs.csv")
    assert os.path.exists(us_csv)

    # 1. Format sniffing
    fmt = sniff_csv_format(us_csv)
    assert fmt["delimiter"] == ","
    assert fmt["has_header"] is True
    assert "job_title" in fmt["columns"]
    assert "salary_estimate" in fmt["columns"]

    # Read sample rows
    rows = []
    with open(us_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    samples_by_col = {col: [r[col] for r in rows[:20]] for col in fmt["columns"]}

    # 2. Semantic column mapping
    mapping = generate_schema_mapping(fmt["columns"], samples_by_col)
    assert mapping["job_title"]["target"] == "job_title"
    assert mapping["company_name"]["target"] == "company"
    assert mapping["job_location"]["target"] == "location"
    assert mapping["salary_estimate"]["target"] == "salary"
    assert mapping["required_skills"]["target"] == "technical_skills"

    # 3. Market Detection
    market = detect_market(rows[:50])
    assert market["country"] == "US"
    assert market["currency"] == "USD"
    assert market["language"] == "en"
    assert market["confidence"] >= 0.80

    # 4. Capability Matrix
    caps = build_capabilities(
        total_records=len(rows),
        valid_counts={
            "job_title": len(rows),
            "salary_monthly_vnd": len(rows),
            "province": len(rows),
            "level": len(rows),
            "technical_skills": len(rows),
            "posted_date": len(rows)
        }
    )
    assert caps["modules"]["overview"]["enabled"] is True
    assert caps["modules"]["hot_jobs"]["enabled"] is True
    assert caps["modules"]["salary"]["enabled"] is True
    assert caps["modules"]["geo"]["enabled"] is True
    assert caps["modules"]["skills"]["enabled"] is True


def test_no_salary_graceful_degradation():
    """CP-32: Validates graceful degradation when dataset lacks salary information."""
    no_sal_csv = os.path.join(PROJECT_ROOT, "tests", "fixtures", "no_salary.csv")
    assert os.path.exists(no_sal_csv)

    # 1. Format sniffing
    fmt = sniff_csv_format(no_sal_csv)
    assert fmt["delimiter"] == ","
    assert "position_title" in fmt["columns"]
    assert "salary" not in str(fmt["columns"]).lower()

    # Read sample rows
    rows = []
    with open(no_sal_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    samples_by_col = {col: [r[col] for r in rows[:20]] for col in fmt["columns"]}

    # 2. Semantic column mapping
    mapping = generate_schema_mapping(fmt["columns"], samples_by_col)
    assert mapping["position_title"]["target"] == "job_title"
    assert mapping["organization"]["target"] == "company"
    assert mapping["work_location"]["target"] == "location"
    assert mapping["skills_required"]["target"] == "technical_skills"

    # Verify no column mapped to salary
    targets = [info["target"] for info in mapping.values()]
    assert "salary" not in targets

    # 3. Market Detection
    market = detect_market(rows[:50])
    assert market["country"] == "VN"
    assert market["language"] == "vi"

    # 4. Capability Matrix: Salary should be disabled with helpful message, others enabled
    caps = build_capabilities(
        total_records=len(rows),
        valid_counts={
            "job_title": len(rows),
            "salary_monthly_vnd": 0,  # 0% coverage
            "province": len(rows),
            "level": len(rows),
            "technical_skills": len(rows),
            "posted_date": len(rows)
        }
    )
    # Salary module is disabled gracefully
    assert caps["modules"]["salary"]["enabled"] is False
    assert "Cần ánh xạ cột mức lương" in caps["modules"]["salary"]["unlock_hint"]
    assert caps["modules"]["ml_salary"]["enabled"] is False

    # Other modules remain fully functional
    assert caps["modules"]["overview"]["enabled"] is True
    assert caps["modules"]["hot_jobs"]["enabled"] is True
    assert caps["modules"]["geo"]["enabled"] is True
    assert caps["modules"]["requirements"]["enabled"] is True
    assert caps["modules"]["skills"]["enabled"] is True
