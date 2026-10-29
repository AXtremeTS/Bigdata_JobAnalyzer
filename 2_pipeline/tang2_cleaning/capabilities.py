# etl/jobmarket_etl/capabilities.py
"""
Capability Matrix Engine:
Determines which analytical modules can be safely activated based on data completeness
and statistical significance thresholds (e.g. coverage >= 30% and n >= 30).
"""

from typing import Dict, Any


def build_capabilities(
    total_records: int,
    valid_counts: Dict[str, int]
) -> Dict[str, Any]:
    """
    Computes capability status for every dashboard module.
    Returns:
        {
            "modules": {
                "<module_name>": {
                    "enabled": bool,
                    "coverage_pct": float,
                    "n_rows": int,
                    "reason": str,
                    "unlock_hint": str
                }
            }
        }
    """
    min_rows_threshold = 30
    min_cov_threshold = 30.0

    def check_field(field_name: str, module_title: str, unlock_hint: str) -> Dict[str, Any]:
        n = valid_counts.get(field_name, 0)
        cov = round((n / float(total_records)) * 100.0, 1) if total_records > 0 else 0.0
        enabled = cov >= min_cov_threshold and n >= min_rows_threshold
        
        reason = f"Đạt độ phủ {cov}% ({n:,} dòng hợp lệ)" if enabled else f"Độ phủ {cov}% dưới ngưỡng {min_cov_threshold}% hoặc ít hơn {min_rows_threshold} dòng"
        return {
            "enabled": enabled,
            "coverage_pct": cov,
            "n_rows": n,
            "reason": reason,
            "unlock_hint": unlock_hint if not enabled else ""
        }

    modules = {}

    # 1. Overview module
    modules["overview"] = {
        "enabled": total_records > 0,
        "coverage_pct": 100.0 if total_records > 0 else 0.0,
        "n_rows": total_records,
        "reason": f"Dữ liệu có {total_records:,} bản ghi",
        "unlock_hint": "Tải lên file CSV chứa ít nhất 1 bản ghi" if total_records == 0 else ""
    }

    # 2. Hot Jobs module
    modules["hot_jobs"] = check_field(
        "job_title",
        "Hot Jobs",
        "Cần ánh xạ cột chứa chức danh việc làm (job_title / vị trí tuyển dụng)"
    )

    # 3. Salary Analysis module
    modules["salary"] = check_field(
        "salary_monthly_vnd",
        "Phân tích lương",
        "Cần ánh xạ cột mức lương (salary / mức thu nhập) có dữ liệu số hoặc dải lương"
    )

    # 4. Geographic Analysis module
    modules["geo"] = check_field(
        "province",
        "Phân tích địa lý",
        "Cần ánh xạ cột địa điểm làm việc (location / tỉnh thành / nơi làm việc)"
    )

    # 5. Requirements module (level, education, experience)
    exp_n = valid_counts.get("exp_min_years", 0)
    level_n = valid_counts.get("level", 0)
    edu_n = valid_counts.get("education", 0)
    best_req_n = max(exp_n, level_n, edu_n)
    req_cov = round((best_req_n / float(total_records)) * 100.0, 1) if total_records > 0 else 0.0
    req_enabled = req_cov >= min_cov_threshold and best_req_n >= min_rows_threshold
    modules["requirements"] = {
        "enabled": req_enabled,
        "coverage_pct": req_cov,
        "n_rows": best_req_n,
        "reason": f"Đạt độ phủ yêu cầu tuyển dụng {req_cov}% ({best_req_n:,} dòng)",
        "unlock_hint": "Cần ánh xạ ít nhất một trong các cột: kinh nghiệm, cấp bậc hoặc học vấn" if not req_enabled else ""
    }

    # 6. Skills module
    skills_n = valid_counts.get("technical_skills", 0)
    skills_cov = round((skills_n / float(total_records)) * 100.0, 1) if total_records > 0 else 0.0
    skills_enabled = skills_cov >= min_cov_threshold and skills_n >= min_rows_threshold
    modules["skills"] = {
        "enabled": skills_enabled,
        "coverage_pct": skills_cov,
        "n_rows": skills_n,
        "reason": f"Đạt độ phủ kỹ năng {skills_cov}% ({skills_n:,} dòng)",
        "unlock_hint": "Cần ánh xạ cột kỹ năng hoặc từ khóa công nghệ (skills / tags)" if not skills_enabled else ""
    }

    # 7. Trend module
    modules["trend"] = check_field(
        "posted_date",
        "Xu hướng thời gian",
        "Cần ánh xạ cột ngày đăng tuyển (posted_date / ngày tạo) với định dạng ngày hợp lệ"
    )

    # 8. Machine Learning: Salary Regression (requires >= 200 rows with valid salary)
    salary_n = valid_counts.get("salary_monthly_vnd", 0)
    ml_salary_enabled = salary_n >= 200
    modules["ml_salary"] = {
        "enabled": ml_salary_enabled,
        "coverage_pct": round((salary_n / float(total_records)) * 100.0, 1) if total_records > 0 else 0.0,
        "n_rows": salary_n,
        "reason": f"Có {salary_n:,} mẫu lương phục vụ huấn luyện mô hình hồi quy (ngưỡng >= 200)" if ml_salary_enabled else f"Chỉ có {salary_n:,} mẫu lương, chưa đủ tối thiểu 200 mẫu để train mô hình hồi quy",
        "unlock_hint": "Cần bổ sung dữ liệu có thông tin mức lương rõ ràng để huấn luyện mô hình ML" if not ml_salary_enabled else ""
    }

    # 9. Machine Learning: Market Clustering
    geo_n = valid_counts.get("province", 0)
    ml_cluster_enabled = total_records >= 100 and geo_n >= 50
    modules["ml_clustering"] = {
        "enabled": ml_cluster_enabled,
        "coverage_pct": round((geo_n / float(total_records)) * 100.0, 1) if total_records > 0 else 0.0,
        "n_rows": geo_n,
        "reason": f"Đủ quy mô dữ liệu ({geo_n:,} vị trí) để chạy thuật toán gom cụm K-Means" if ml_cluster_enabled else "Chưa đủ số lượng dữ liệu địa lý đa dạng để chạy gom cụm",
        "unlock_hint": "Cần ít nhất 100 bản ghi kèm thông tin tỉnh/thành phố" if not ml_cluster_enabled else ""
    }

    return {
        "total_records": total_records,
        "min_coverage_threshold": min_cov_threshold,
        "modules": modules
    }
