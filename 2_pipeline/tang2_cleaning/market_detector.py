# etl/jobmarket_etl/market_detector.py
"""
Market Detector module:
Infers country, primary currency, and language of a job posting dataset
based on geographic cues, currency tokens, and text linguistic markers.
"""

import re
from typing import Dict, List, Any


def detect_market(sample_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analyzes sample rows and outputs market metadata with confidence scores.
    """
    evidence = []
    
    # Aggregated text tokens
    full_sample_text = ""
    for r in sample_records[:100]:
        for val in r.values():
            if val:
                full_sample_text += " " + str(val).lower()

    # 1. Country Detection
    vn_count = len(re.findall(r"\b(việt nam|vietnam|hà nội|hồ chí minh|đà nẵng|tphcm)\b", full_sample_text))
    us_count = len(re.findall(r"\b(united states|usa|new york|california|san francisco|chicago)\b", full_sample_text))
    uk_count = len(re.findall(r"\b(united kingdom|london|manchester|birmingham|uk)\b", full_sample_text))

    if vn_count >= us_count and vn_count >= uk_count and vn_count > 0:
        country = "VN"
        country_name = "Việt Nam"
        country_conf = min(0.99, 0.6 + (vn_count / 100.0))
        evidence.append(f"Detected {vn_count} Vietnam geographic tokens")
    elif us_count > uk_count:
        country = "US"
        country_name = "United States"
        country_conf = min(0.99, 0.6 + (us_count / 100.0))
        evidence.append(f"Detected {us_count} US geographic tokens")
    elif uk_count > 0:
        country = "UK"
        country_name = "United Kingdom"
        country_conf = min(0.99, 0.6 + (uk_count / 100.0))
        evidence.append(f"Detected {uk_count} UK geographic tokens")
    else:
        country = "UNKNOWN"
        country_name = "Unknown Market"
        country_conf = 0.5
        evidence.append("No definitive geographic signal detected; defaulting to agnostic")

    # 2. Currency Detection
    vnd_signals = len(re.findall(r"\b(triệu|vnd|đ|dong|đồng)\b", full_sample_text))
    usd_signals = len(re.findall(r"(\$|\busd\b)", full_sample_text))
    gbp_signals = len(re.findall(r"(£|\bgbp\b)", full_sample_text))

    if vnd_signals >= usd_signals and vnd_signals >= gbp_signals and vnd_signals > 0:
        currency = "VND"
        curr_conf = min(0.99, 0.6 + (vnd_signals / 50.0))
        evidence.append(f"Detected {vnd_signals} VND currency tokens ('triệu', 'vnd')")
    elif usd_signals > gbp_signals:
        currency = "USD"
        curr_conf = min(0.99, 0.6 + (usd_signals / 50.0))
        evidence.append(f"Detected {usd_signals} USD currency symbols ('$')")
    elif gbp_signals > 0:
        currency = "GBP"
        curr_conf = min(0.99, 0.6 + (gbp_signals / 50.0))
        evidence.append(f"Detected {gbp_signals} GBP currency symbols ('£')")
    else:
        currency = "VND" if country == "VN" else "USD"
        curr_conf = 0.6
        evidence.append(f"Defaulting currency to {currency} based on country {country}")

    # 3. Language Detection
    vi_markers = len(re.findall(r"\b(nhân viên|công ty|kinh nghiệm|yêu cầu|quyền lợi|toàn thời gian|thực tập)\b", full_sample_text))
    en_markers = len(re.findall(r"\b(engineer|developer|manager|experience|requirements|benefits|full-time)\b", full_sample_text))

    if vi_markers >= en_markers and vi_markers > 0:
        language = "vi"
        lang_conf = min(0.99, 0.6 + (vi_markers / 50.0))
        evidence.append(f"Detected {vi_markers} Vietnamese keyword markers")
    elif en_markers > 0:
        language = "en"
        lang_conf = min(0.99, 0.6 + (en_markers / 50.0))
        evidence.append(f"Detected {en_markers} English keyword markers")
    else:
        language = "vi" if country == "VN" else "en"
        lang_conf = 0.5
        evidence.append(f"Inferred language '{language}' from country code")

    overall_confidence = round((country_conf + curr_conf + lang_conf) / 3.0, 2)

    return {
        "country": country,
        "country_name": country_name,
        "currency": currency,
        "language": language,
        "confidence": overall_confidence,
        "evidence": evidence
    }
