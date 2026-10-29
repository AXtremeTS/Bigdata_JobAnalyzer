# scripts/generate_fixtures.py
"""
Generates synthetic multi-dataset fixtures for CP-31 (US/UK jobs) and CP-32 (no salary dataset)
to validate Schema Adaptability and Graceful Degradation in JobMarket Intelligence.
"""

import csv
import random
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURES_DIR = os.path.join(PROJECT_ROOT, "tests", "fixtures")
os.makedirs(FIXTURES_DIR, exist_ok=True)

# 1. Generate us_jobs.csv (CP-31: US/UK international dataset)
us_titles = [
    "Senior Software Engineer", "Data Scientist", "DevOps Engineer", "Cloud Solutions Architect",
    "Frontend Developer", "Backend Engineer", "Machine Learning Engineer", "Product Manager",
    "Cybersecurity Analyst", "Data Engineer", "QA Automation Engineer", "Full Stack Developer"
]
us_companies = [
    "Amazon Web Services", "Google Cloud", "Microsoft Corp", "Meta Platforms",
    "Netflix Inc", "Apple Inc", "Stripe Inc", "Salesforce", "Snowflake", "Databricks"
]
us_locations = [
    "San Francisco, CA", "Seattle, WA", "New York, NY", "Austin, TX",
    "Boston, MA", "Chicago, IL", "Denver, CO", "Los Angeles, CA", "Remote, US"
]
us_salaries = [
    "$120,000 - $160,000 / yr", "$140,000 a year", "$95,000 - $130,000 / year",
    "$180,000 - $220,000 / yr", "$80 - $110 / hr", "$110,000 - $145,000 / yr",
    "$135,000 a year", "$165,000 - $195,000 / yr"
]
us_skills = [
    "Python, AWS, Docker, Kubernetes, PostgreSQL",
    "Java, Spring Boot, Microservices, Kafka, Redis",
    "React, TypeScript, Next.js, Tailwind CSS, GraphQL",
    "Python, PyTorch, TensorFlow, Scikit-learn, MLOps",
    "Terraform, AWS, CI/CD, Linux, Prometheus",
    "SQL, Snowflake, dbt, Apache Spark, Airflow"
]
us_levels = ["Senior", "Lead", "Staff", "Mid-Level", "Director", "Entry-Level"]

us_file = os.path.join(FIXTURES_DIR, "us_jobs.csv")
with open(us_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "job_title", "company_name", "job_location", "salary_estimate",
        "experience_level", "required_skills", "posted_date"
    ])
    for i in range(120):
        writer.writerow([
            random.choice(us_titles),
            random.choice(us_companies),
            random.choice(us_locations),
            random.choice(us_salaries),
            random.choice(us_levels),
            random.choice(us_skills),
            f"2026-03-{random.randint(1, 28):02d}"
        ])

print(f"Generated {us_file} with 120 rows.")

# 2. Generate no_salary.csv (CP-32: Missing salary column dataset)
vn_titles = [
    "Kỹ sư Lập trình Java", "Chuyên viên Phân tích Dữ liệu", "Frontend React Developer",
    "Kỹ sư DevOps / Cloud", "Nhân viên Kiểm thử Phần mềm (QA/QC)", "Trưởng nhóm Kỹ thuật (Tech Lead)",
    "Chuyên viên An toàn Thông tin", "Lập trình viên Python Backend"
]
vn_orgs = [
    "Tập đoàn FPT", "Viettel Telecom", "VNG Corporation", "Momo (M-Service)",
    "Tiki Corporation", "VNPT IT", "Shopee Vietnam", "KMS Technology"
]
vn_locations = [
    "TP Hồ Chí Minh", "Hà Nội", "Đà Nẵng", "Cần Thơ", "Bình Dương", "Quận 1, TP Hồ Chí Minh", "Cầu Giấy, Hà Nội"
]
vn_skills = [
    "Java, Spring Boot, Oracle, Docker",
    "Python, SQL, Power BI, Excel",
    "ReactJS, JavaScript, HTML5, CSS3",
    "Linux, Docker, Kubernetes, AWS",
    "Manual Testing, Selenium, Postman, JIRA",
    "Architecture, Microservices, Leadership, CI/CD"
]
vn_levels = ["Nhân viên", "Trưởng nhóm", "Chuyên viên", "Fresher", "Senior"]
vn_edu = ["Đại học", "Cao đẳng", "Cử nhân", "Kỹ sư", "Thạc sĩ"]

no_sal_file = os.path.join(FIXTURES_DIR, "no_salary.csv")
with open(no_sal_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "position_title", "organization", "work_location", "job_level",
        "skills_required", "education_level", "created_at"
    ])
    for i in range(80):
        writer.writerow([
            random.choice(vn_titles),
            random.choice(vn_orgs),
            random.choice(vn_locations),
            random.choice(vn_levels),
            random.choice(vn_skills),
            random.choice(vn_edu),
            f"2026-04-{random.randint(1, 28):02d}"
        ])

print(f"Generated {no_sal_file} with 80 rows.")
