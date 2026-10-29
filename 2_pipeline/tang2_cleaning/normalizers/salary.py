# etl/jobmarket_etl/normalizers/salary.py
"""
Salary Normalizer module:
Parses and standardizes arbitrary salary strings into monthly VND and USD values.
Handles range (min-max), single bound ('Up to', 'Trên'), currencies (VND, USD, GBP, EUR),
periods (hourly, weekly, monthly, annual), and negotiable cases ('Thoả thuận').
"""

import re
from typing import Dict, Any, Optional

FX_RATES = {
    "VND": 1.0,
    "USD": 25450.0,
    "EUR": 27200.0,
    "GBP": 32500.0
}


def normalize_salary(raw_val: Optional[str]) -> Dict[str, Any]:
    """
    Parses a raw salary string into structured monthly salary values in VND and USD.
    """
    result = {
        "salary_raw": raw_val,
        "salary_min_vnd": None,
        "salary_max_vnd": None,
        "salary_avg_vnd": None,
        "salary_min_usd": None,
        "salary_max_usd": None,
        "salary_avg_usd": None,
        "currency": "VND",
        "period": "month",
        "is_negotiable": False,
        "is_outlier": False
    }

    if not raw_val or str(raw_val).strip() == "":
        return result

    val = str(raw_val).strip().lower()

    # Check for Negotiable / Thoả thuận
    if re.search(r"\b(thoả thuận|thoa thuan|negotiable|thương lượng|thuong luong|cạnh tranh|competitive)\b", val):
        result["is_negotiable"] = True
        return result

    # Detect currency
    currency = "VND"
    if "$" in val or "usd" in val:
        currency = "USD"
    elif "£" in val or "gbp" in val:
        currency = "GBP"
    elif "€" in val or "eur" in val:
        currency = "EUR"
    result["currency"] = currency

    # Detect period
    period = "month"
    if re.search(r"\b(per annum|năm|nam|year|annually|\/year|\/năm)\b", val):
        period = "year"
    elif re.search(r"\b(giờ|gio|hour|hourly|\/h|\/hour)\b", val):
        period = "hour"
    elif re.search(r"\b(ngày|ngay|day|daily)\b", val):
        period = "day"
    result["period"] = period

    # Check for multiplier (triệu / million / k)
    multiplier = 1.0
    if "triệu" in val or "trieu" in val or "tr" in val or "m" in val:
        multiplier = 1_000_000.0
    elif "k" in val:
        multiplier = 1_000.0

    # Extract all floating point / integer numbers
    # Handle thousands separators: comma or dot followed by exactly 3 digits (e.g. 1,500 or 15.000.000)
    clean_num_str = re.sub(r",(\d{3})(?=\D|$)", r"\1", val)
    clean_num_str = re.sub(r"\.(\d{3})(?=\D|$)", r"\1", clean_num_str)
    # Handle decimal comma followed by 1 or 2 digits (e.g. 15,5 triệu -> 15.5)
    clean_num_str = re.sub(r"(\d+),(\d{1,2})(?=\D|$)", r"\1.\2", clean_num_str)
    
    nums = [float(n) for n in re.findall(r"\b\d+(?:\.\d+)?\b", clean_num_str)]

    if not nums:
        result["is_negotiable"] = True
        return result

    s_min = None
    s_max = None

    if len(nums) >= 2:
        s_min = min(nums[0], nums[1])
        s_max = max(nums[0], nums[1])
    elif len(nums) == 1:
        if "tới" in val or "đến" in val or "up to" in val or "<" in val:
            s_min = 0.0
            s_max = nums[0]
        elif "trên" in val or "từ" in val or ">" in val or "from" in val:
            s_min = nums[0]
            s_max = nums[0] * 1.5  # Heuristic estimation for open-ended top
        else:
            s_min = nums[0]
            s_max = nums[0]

    # If numbers are small (< 500) and currency is VND, multiply by million
    if currency == "VND":
        if s_max and s_max < 1000:
            s_min = (s_min or 0) * 1_000_000.0
            s_max = (s_max or 0) * 1_000_000.0
        elif multiplier > 1.0 and s_max and s_max < 10_000:
            s_min = (s_min or 0) * multiplier
            s_max = (s_max or 0) * multiplier

    # If currency is USD and format was '60k - 80k'
    if currency in ("USD", "GBP", "EUR") and "k" in val:
        if s_min and s_min < 1000:
            s_min *= 1000.0
        if s_max and s_max < 1000:
            s_max *= 1000.0

    # Convert period to Monthly
    if period == "year":
        if s_min: s_min /= 12.0
        if s_max: s_max /= 12.0
    elif period == "hour":
        # Standard 160 working hours/month
        if s_min: s_min *= 160.0
        if s_max: s_max *= 160.0
    elif period == "day":
        if s_min: s_min *= 22.0
        if s_max: s_max *= 22.0

    # Calculate VND and USD monthly equivalents
    rate_to_vnd = FX_RATES.get(currency, 1.0)
    usd_rate = FX_RATES["USD"]

    if s_min is not None and s_max is not None:
        s_avg = (s_min + s_max) / 2.0
        
        # Monthly VND
        result["salary_min_vnd"] = round(s_min * rate_to_vnd)
        result["salary_max_vnd"] = round(s_max * rate_to_vnd)
        result["salary_avg_vnd"] = round(s_avg * rate_to_vnd)

        # Monthly USD
        result["salary_min_usd"] = round(result["salary_min_vnd"] / usd_rate, 2)
        result["salary_max_usd"] = round(result["salary_max_vnd"] / usd_rate, 2)
        result["salary_avg_usd"] = round(result["salary_avg_vnd"] / usd_rate, 2)

        # Outlier detection (salary > 500 million VND/month or < 1 million VND/month)
        if result["salary_avg_vnd"] > 500_000_000 or (result["salary_avg_vnd"] > 0 and result["salary_avg_vnd"] < 1_000_000):
            result["is_outlier"] = True

    return result
