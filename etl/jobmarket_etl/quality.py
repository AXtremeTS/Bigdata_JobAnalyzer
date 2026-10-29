# etl/jobmarket_etl/quality.py
"""
Data Quality Engine module:
Audits field completeness, null rates, detects duplicates, identifies quarantine records,
and calculates an overall dataset quality score.
"""

from typing import Dict, List, Any, Optional


def evaluate_quality_metrics(
    total_records: int,
    null_counts: Dict[str, int],
    duplicate_count: int,
    quarantine_count: int,
    quarantine_reasons: Optional[Dict[str, int]] = None
) -> Dict[str, Any]:
    """
    Computes a comprehensive quality report and overall score (0 - 100).
    """
    if total_records == 0:
        return {
            "total_records": 0,
            "overall_score": 0.0,
            "grade": "F",
            "duplicate_count": 0,
            "duplicate_pct": 0.0,
            "quarantine_count": 0,
            "quarantine_pct": 0.0,
            "field_completeness": {},
            "quarantine_reasons": {}
        }

    # Completeness per field
    field_completeness = {}
    total_null_pct = 0.0
    key_fields = ["job_title", "company", "salary_monthly_vnd", "province", "level", "technical_skills"]

    for field, null_c in null_counts.items():
        null_pct = round((null_c / float(total_records)) * 100.0, 2)
        valid_pct = round(100.0 - null_pct, 2)
        field_completeness[field] = {
            "null_count": null_c,
            "null_pct": null_pct,
            "valid_pct": valid_pct
        }
        if field in key_fields:
            total_null_pct += null_pct

    # Calculate overall quality score (0 - 100)
    # Penalties:
    # - Average null rate in key fields (weight: 40%)
    # - Duplicate rate (weight: 30%)
    # - Quarantine rate (weight: 30%)
    avg_key_null_pct = total_null_pct / float(len(key_fields)) if key_fields else 0.0
    dup_pct = round((duplicate_count / float(total_records)) * 100.0, 2)
    quar_pct = round((quarantine_count / float(total_records)) * 100.0, 2)

    completeness_score = max(0.0, 100.0 - avg_key_null_pct)
    uniqueness_score = max(0.0, 100.0 - (dup_pct * 3.0))
    validity_score = max(0.0, 100.0 - (quar_pct * 4.0))

    overall_score = round(
        (completeness_score * 0.40) + (uniqueness_score * 0.30) + (validity_score * 0.30),
        1
    )

    grade = "A"
    if overall_score < 50:
        grade = "D"
    elif overall_score < 70:
        grade = "C"
    elif overall_score < 85:
        grade = "B"

    return {
        "total_records": total_records,
        "overall_score": overall_score,
        "grade": grade,
        "duplicate_count": duplicate_count,
        "duplicate_pct": dup_pct,
        "quarantine_count": quarantine_count,
        "quarantine_pct": quar_pct,
        "field_completeness": field_completeness,
        "quarantine_reasons": quarantine_reasons or {}
    }
