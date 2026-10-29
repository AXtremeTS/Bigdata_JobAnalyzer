# etl/jobmarket_etl/normalizers/experience.py
"""
Experience Normalizer module:
Extracts minimum and maximum required years of experience from raw strings.
Handles patterns in Vietnamese and English:
- "2 năm kinh nghiệm", "1 - 3 năm", "từ 2 đến 5 năm", "Dưới 1 năm", "Trên 5 năm"
- "1-3 years", "2+ years experience", "No experience required", "3 to 5 years", "Fresh graduate"
"""

import re
from typing import Dict, Any, Optional


def normalize_experience(raw_val: Optional[str]) -> Dict[str, Any]:
    """
    Parses a raw experience string into structured min and max years.
    Returns:
        {
            "exp_min_years": Optional[float],
            "exp_max_years": Optional[float],
            "exp_raw": Optional[str],
            "is_no_exp_required": bool
        }
    """
    result = {
        "exp_min_years": None,
        "exp_max_years": None,
        "exp_raw": raw_val,
        "is_no_exp_required": False
    }

    if not raw_val or str(raw_val).strip() == "":
        return result

    val = str(raw_val).strip().lower()

    # Check for no experience required
    if re.search(r"\b(không yêu cầu|khong yeu cau|chưa có kinh nghiệm|chua co kinh nghiem|không cần kinh nghiệm|khong can|no experience|fresher|entry level|mới tốt nghiệp)\b", val):
        result["is_no_exp_required"] = True
        result["exp_min_years"] = 0.0
        result["exp_max_years"] = 0.0
        return result

    # Standardize ranges like "1-3" or "1 - 3" or "từ 1 đến 3"
    nums = [float(n) for n in re.findall(r"\b\d+(?:\.\d+)?\b", val)]

    if not nums:
        return result

    if len(nums) >= 2:
        result["exp_min_years"] = min(nums[0], nums[1])
        result["exp_max_years"] = max(nums[0], nums[1])
    elif len(nums) == 1:
        n = nums[0]
        if re.search(r"\b(dưới|duoi|tối đa|toi da|up to|less than|<)\b", val):
            result["exp_min_years"] = 0.0
            result["exp_max_years"] = n
        elif re.search(r"\b(trên|tren|tối thiểu|toi thieu|từ|tu|at least|minimum|>|\+)\b", val) or "+" in val:
            result["exp_min_years"] = n
            result["exp_max_years"] = n + 3.0  # reasonable upper bound estimation
        else:
            result["exp_min_years"] = n
            result["exp_max_years"] = n

    # Sanity checks (cap unreasonable experience numbers > 30 years)
    if result["exp_min_years"] is not None and result["exp_min_years"] > 35:
        result["exp_min_years"] = None
        result["exp_max_years"] = None
    elif result["exp_max_years"] is not None and result["exp_max_years"] > 35:
        result["exp_max_years"] = 35.0

    return result
