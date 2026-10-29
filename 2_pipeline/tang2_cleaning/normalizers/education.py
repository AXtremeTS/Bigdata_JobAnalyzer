# etl/jobmarket_etl/normalizers/education.py
"""
Education Normalizer module:
Standardizes raw education strings into canonical academic credentials:
- Tiến sĩ (Doctorate / PhD)
- Thạc sĩ (Master / Post-graduate)
- Đại học (Bachelor / University Degree / Cử nhân / Kỹ sư)
- Cao đẳng (College / Associate Degree)
- Trung cấp (Vocational / Intermediate)
- THPT (High School / Cấp 3)
- Không yêu cầu (Not Required)
"""

import re
from typing import Optional

EDU_PATTERNS = [
    (r"\b(tiến sĩ|tien si|phd|doctorate)\b", "Tiến sĩ"),
    (r"\b(thạc sĩ|thac si|master|cao học|cao hoc|sau đại học)\b", "Thạc sĩ"),
    (r"\b(đại học|dai hoc|cử nhân|cu nhan|kỹ sư|ky su|bachelor|university|degree)\b", "Đại học"),
    (r"\b(cao đẳng|cao dang|college|associate)\b", "Cao đẳng"),
    (r"\b(trung cấp|trung cap|nghề|nghe|vocational|intermediate)\b", "Trung cấp"),
    (r"\b(thpt|trung học|trung hoc|cấp 3|cap 3|high school)\b", "THPT"),
    (r"\b(không yêu cầu|khong yeu cau|không đòi hỏi|chưa tốt nghiệp|bất kỳ|any)\b", "Không yêu cầu")
]


def normalize_education(raw_val: Optional[str]) -> str:
    """
    Maps raw education description into canonical educational level.
    """
    if not raw_val or str(raw_val).strip() == "":
        return "Không yêu cầu"

    val = str(raw_val).strip().lower()

    for pattern, std_edu in EDU_PATTERNS:
        if re.search(pattern, val):
            return std_edu

    return "Không yêu cầu"
