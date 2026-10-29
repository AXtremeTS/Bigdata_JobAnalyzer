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
    my_count = len(re.findall(
        r"\b(malaysia|malaysian|kuala lumpur|penang|penang island|johor|selangor|petaling|sarawak|sabah)\b",
        full_sample_text
    ))
    sg_count = len(re.findall(r"\b(singapore|singaporean)\b", full_sample_text))

    country_scores = {"VN": vn_count, "US": us_count, "UK": uk_count, "MY": my_count, "SG": sg_count}
    country = max(country_scores, key=country_scores.get)
    if country_scores[country] > 0:
        country_names = {
            "VN": "Việt Nam",
            "US": "United States",
            "UK": "United Kingdom",
            "MY": "Malaysia",
            "SG": "Singapore",
        }
        country_name = country_names[country]
        country_conf = min(0.99, 0.6 + (country_scores[country] / 100.0))
        evidence.append(f"Detected {country_scores[country]} {country_name} geographic tokens")
    else:
        country = "VN"
        country_name = "Unknown Market"
        country_conf = 0.5
        evidence.append("No definitive geographic signal detected; defaulting to agnostic")

    # 2. Currency Detection
    vnd_signals = len(re.findall(r"\b(triệu|vnd|đ|dong|đồng)\b", full_sample_text))
    usd_signals = len(re.findall(r"(\$|\busd\b)", full_sample_text))
    gbp_signals = len(re.findall(r"(£|\bgbp\b)", full_sample_text))
    myr_signals = len(re.findall(r"(\bmyr\b|\brm\s?\d|\bringgit)", full_sample_text))
    sgd_signals = len(re.findall(r"(\bsgd\b|\bs\$\s?\d)", full_sample_text))

    currency_scores = {"VND": vnd_signals, "USD": usd_signals, "GBP": gbp_signals, "MYR": myr_signals, "SGD": sgd_signals}
    currency = max(currency_scores, key=currency_scores.get)
    if currency_scores[currency] > 0:
        curr_conf = min(0.99, 0.6 + (currency_scores[currency] / 50.0))
        evidence.append(f"Detected {currency_scores[currency]} {currency} currency signals")
    else:
        currency = "VND" if country == "VN" else ("MYR" if country == "MY" else ("SGD" if country == "SG" else "USD"))
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
