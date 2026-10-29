# tests/test_normalizers.py
"""
Comprehensive unit tests for jobmarket_etl normalizers, mapper, sniffer,
market detector, quality engine, and capability matrix.
"""

import pytest
import sys
import os

# Add etl directory to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "etl"))

from jobmarket_etl.normalizers.salary import normalize_salary
from jobmarket_etl.normalizers.location import normalize_location
from jobmarket_etl.normalizers.level import normalize_level
from jobmarket_etl.normalizers.experience import normalize_experience
from jobmarket_etl.normalizers.education import normalize_education
from jobmarket_etl.normalizers.skills import normalize_skills
from jobmarket_etl.mapper import map_column, generate_schema_mapping
from jobmarket_etl.market_detector import detect_market
from jobmarket_etl.quality import evaluate_quality_metrics
from jobmarket_etl.capabilities import build_capabilities


class TestSalaryNormalizer:
    def test_vnd_range(self):
        res = normalize_salary("15 - 20 triệu")
        assert res["salary_min_vnd"] == 15_000_000
        assert res["salary_max_vnd"] == 20_000_000
        assert res["salary_avg_vnd"] == 17_500_000
        assert res["currency"] == "VND"
        assert not res["is_negotiable"]
        assert not res["is_outlier"]

    def test_negotiable_vietnamese(self):
        res = normalize_salary("Thoả thuận")
        assert res["is_negotiable"] is True
        assert res["salary_min_vnd"] is None

    def test_negotiable_english(self):
        res = normalize_salary("Competitive / Negotiable")
        assert res["is_negotiable"] is True
        assert res["salary_min_vnd"] is None

    def test_usd_monthly(self):
        res = normalize_salary("$1,500 - $2,500")
        assert res["currency"] == "USD"
        assert res["salary_min_usd"] == 1500.0
        assert res["salary_max_usd"] == 2500.0
        assert res["salary_min_vnd"] > 0

    def test_up_to_bound(self):
        res = normalize_salary("Up to 30 triệu")
        assert res["salary_min_vnd"] == 0
        assert res["salary_max_vnd"] == 30_000_000


class TestLocationNormalizer:
    def test_hcm_district(self):
        res = normalize_location("Quận 1, TP.HCM")
        assert res["province"] == "Hồ Chí Minh"
        assert res["region"] == "Nam"
        assert "Quận 1" in (res["district"] or "")

    def test_hanoi(self):
        res = normalize_location("Hà Nội")
        assert res["province"] == "Hà Nội"
        assert res["region"] == "Bắc"

    def test_danang(self):
        res = normalize_location("Đà Nẵng")
        assert res["province"] == "Đà Nẵng"
        assert res["region"] == "Trung"

    def test_multi_location(self):
        res = normalize_location("Hà Nội, Hồ Chí Minh")
        assert res["is_multi_location"] is True


class TestLevelNormalizer:
    def test_senior_level(self):
        assert normalize_level("Senior Python Engineer") == "Senior"

    def test_intern_level(self):
        assert normalize_level("Thực tập sinh Data Analyst") == "Intern"

    def test_manager_level(self):
        assert normalize_level("Trưởng phòng Kỹ thuật") == "Manager/Lead"


class TestExperienceNormalizer:
    def test_exp_range(self):
        res = normalize_experience("1 - 3 năm kinh nghiệm")
        assert res["exp_min_years"] == 1.0
        assert res["exp_max_years"] == 3.0
        assert res["is_no_exp_required"] is False

    def test_no_experience(self):
        res = normalize_experience("Không yêu cầu kinh nghiệm")
        assert res["is_no_exp_required"] is True
        assert res["exp_min_years"] == 0.0

    def test_single_plus_year(self):
        res = normalize_experience("3+ years experience")
        assert res["exp_min_years"] == 3.0
        assert res["exp_max_years"] >= 3.0


class TestEducationNormalizer:
    def test_bachelor(self):
        assert normalize_education("Tốt nghiệp Đại học chuyên ngành CNTT") == "Đại học"

    def test_college(self):
        assert normalize_education("Cao đẳng") == "Cao đẳng"

    def test_none_required(self):
        assert normalize_education("Không yêu cầu bằng cấp") == "Không yêu cầu"


class TestSkillsNormalizer:
    def test_python_list_format(self):
        raw = "['Python', 'SQL', 'reactjs', 'Docker']"
        skills = normalize_skills(raw)
        assert "Python" in skills
        assert "SQL" in skills
        assert "React" in skills
        assert "Docker" in skills

    def test_semicolon_format(self):
        raw = "Java; Spring Boot; postgresql"
        skills = normalize_skills(raw)
        assert "Java" in skills
        assert "Spring Boot" in skills
        assert "PostgreSQL" in skills


class TestMapperAndMarketDetector:
    def test_map_column(self):
        mapping = map_column("Mức lương", ["15-20 triệu", "Thoả thuận"], {"salary": ["mức lương", "lương", "salary"]})
        assert mapping["target"] == "salary"
        assert mapping["confidence"] >= 0.8

    def test_market_detector_vietnam(self):
        sample = [
            {"title": "Kỹ sư dữ liệu", "loc": "Hà Nội", "sal": "20 triệu VND"},
            {"title": "Nhân viên bán hàng", "loc": "Hồ Chí Minh", "sal": "15 triệu"}
        ]
        res = detect_market(sample)
        assert res["country"] == "VN"
        assert res["currency"] == "VND"
        assert res["language"] == "vi"


class TestQualityAndCapabilities:
    def test_quality_metrics(self):
        res = evaluate_quality_metrics(
            total_records=1000,
            null_counts={"job_title": 0, "company": 10, "salary_monthly_vnd": 200},
            duplicate_count=15,
            quarantine_count=5
        )
        assert res["total_records"] == 1000
        assert res["overall_score"] > 80.0
        assert res["grade"] in ("A", "B")

    def test_capabilities_builder(self):
        res = build_capabilities(
            total_records=1000,
            valid_counts={
                "job_title": 1000,
                "company": 980,
                "salary_monthly_vnd": 650,
                "province": 920,
                "technical_skills": 800,
                "level": 700,
                "education": 600,
                "exp_min_years": 750,
                "posted_date": 500
            }
        )
        assert res["modules"]["overview"]["enabled"] is True
        assert res["modules"]["salary"]["enabled"] is True
        assert res["modules"]["geo"]["enabled"] is True
        assert res["modules"]["ml_salary"]["enabled"] is True
