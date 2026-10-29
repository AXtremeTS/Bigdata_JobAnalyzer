# etl/jobmarket_etl/normalizers/level.py
"""
Job Level Normalizer:
Standardizes level into: Intern, Fresher, Junior, Mid, Senior, Lead, Manager, Director.
"""

import re
from typing import Optional

LEVEL_PATTERNS = [
    (r"\b(director|giám đốc|pho giam doc|phó giám đốc|head of|vp|c-level)\b", "Director"),
    (r"\b(manager|trưởng phòng|truong phong|quan ly|quản lý|lead|trưởng nhóm|truong nhom|principal|architect)\b", "Manager/Lead"),
    (r"\b(senior|chuyên viên cao cấp|chuyen vien cao cap|sr\.?|chuyên gia|chuyen gia)\b", "Senior"),
    (r"\b(junior|chuyên viên|chuyen vien|nhân viên|nhan vien|jr\.?|kỹ sư|ky su)\b", "Junior/Mid"),
    (r"\b(fresher|mới tốt nghiệp|moi tot nghiep|entry)\b", "Fresher"),
    (r"\b(intern|thực tập|thuc tap|sinh viên|học việc|trainee)\b", "Intern")
]


def normalize_level(job_title: Optional[str], raw_level: Optional[str] = None) -> str:
    """Infers canonical seniority level from raw level or job title."""
    text = (str(raw_level or "") + " " + str(job_title or "")).lower()
    
    for pattern, std_level in LEVEL_PATTERNS:
        if re.search(pattern, text):
            return std_level
            
    return "Not Specified"
