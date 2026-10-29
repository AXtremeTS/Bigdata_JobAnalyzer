# etl/jobmarket_etl/normalizers/location.py
"""
Location Normalizer module:
Standardizes raw location strings into canonical Province, Region (Bắc/Trung/Nam),
and District (Quận/Huyện).
"""

import json
import os
import re
from typing import Dict, Any, Optional

DEFAULT_GAZETTEER = {
    "regions": {
        "Bắc": ["Hà Nội", "Hải Phòng", "Bắc Ninh", "Quảng Ninh", "Hải Dương", "Hưng Yên", "Vĩnh Phúc", "Thái Nguyên", "Nam Định", "Ninh Bình", "Hà Nam", "Thái Bình", "Phú Thọ", "Bắc Giang", "Lạng Sơn", "Tuyên Quang", "Hà Giang", "Cao Bằng", "Yên Bái", "Lào Cai", "Bắc Kạn", "Điện Biên", "Lai Châu", "Sơn La", "Hòa Bình"],
        "Trung": ["Đà Nẵng", "Thừa Thiên Huế", "Quảng Nam", "Quảng Ngãi", "Bình Định", "Phú Yên", "Khánh Hòa", "Ninh Thuận", "Bình Thuận", "Thanh Hóa", "Nghệ An", "Hà Tĩnh", "Quảng Bình", "Quảng Trị", "Kon Tum", "Gia Lai", "Đắk Lắk", "Đắk Nông", "Lâm Đồng"],
        "Nam": ["Hồ Chí Minh", "TP Hồ Chí Minh", "Bình Dương", "Đồng Nai", "Bà Rịa - Vũng Tàu", "Long An", "Tây Ninh", "Tiền Giang", "Bến Tre", "Vĩnh Long", "Trà Vinh", "Hậu Giang", "Sóc Trăng", "Đồng Tháp", "An Giang", "Kiên Giang", "Bạc Liêu", "Cà Mau", "Cần Thơ", "Bình Phước"]
    },
    "province_aliases": {
        "hồ chí minh": "Hồ Chí Minh", "ho chi minh": "Hồ Chí Minh", "tp.hcm": "Hồ Chí Minh", "tphcm": "Hồ Chí Minh", "hcm": "Hồ Chí Minh", "sài gòn": "Hồ Chí Minh",
        "hà nội": "Hà Nội", "ha noi": "Hà Nội", "hn": "Hà Nội", "hanoi": "Hà Nội",
        "đà nẵng": "Đà Nẵng", "da nang": "Đà Nẵng",
        "bình dương": "Bình Dương", "binh duong": "Bình Dương",
        "đồng nai": "Đồng Nai", "dong nai": "Đồng Nai",
        "vũng tàu": "Bà Rịa - Vũng Tàu", "bà rịa": "Bà Rịa - Vũng Tàu",
        "hải phòng": "Hải Phòng", "hai phong": "Hải Phòng",
        "cần thơ": "Cần Thơ", "can tho": "Cần Thơ",
        "vĩnh long": "Vĩnh Long", "vinh long": "Vĩnh Long",
        "nghệ an": "Nghệ An", "nghe an": "Nghệ An",
        "thanh hóa": "Thanh Hóa", "thanh hoa": "Thanh Hóa",
        "khánh hòa": "Khánh Hòa", "khanh hoa": "Khánh Hòa", "nha trang": "Khánh Hòa",
        "lâm đồng": "Lâm Đồng", "lam dong": "Lâm Đồng", "đà lạt": "Lâm Đồng",
        "tây ninh": "Tây Ninh", "tay ninh": "Tây Ninh",
        "phú thọ": "Phú Thọ", "phu tho": "Phú Thọ",
        "hưng yên": "Hưng Yên", "hung yen": "Hưng Yên",
        "ninh bình": "Ninh Bình", "ninh binh": "Ninh Bình",
        "hà đông": "Hà Nội", "ha dong": "Hà Nội",
        "quảng nam": "Quảng Nam", "quang nam": "Quảng Nam",
        "quảng ngãi": "Quảng Ngãi", "quang ngai": "Quảng Ngãi",
        "bình định": "Bình Định", "binh dinh": "Bình Định",
        "phú yên": "Phú Yên", "phu yen": "Phú Yên",
        "long an": "Long An",
        "tiền giang": "Tiền Giang", "tien giang": "Tiền Giang",
        "bình phước": "Bình Phước", "binh phuoc": "Bình Phước"
    }
}


def get_region_for_province(province: str) -> Optional[str]:
    """Maps province name to macro-region: Bắc, Trung, Nam."""
    for reg, prov_list in DEFAULT_GAZETTEER["regions"].items():
        if province in prov_list:
            return reg
    return None


def normalize_location(raw_val: Optional[str]) -> Dict[str, Any]:
    """
    Parses location text into structured geographical components.
    """
    result = {
        "location_raw": raw_val,
        "province": None,
        "district": None,
        "region": None,
        "is_multi_location": False,
        "country": "Việt Nam"
    }

    if not raw_val or str(raw_val).strip() == "":
        return result

    val = str(raw_val).strip().lower()

    # Check for multiple locations (comma or slash separated cities)
    if "," in val or "/" in val:
        parts = [p.strip() for p in re.split(r"[,/]", val) if p.strip()]
        if len(parts) > 1:
            result["is_multi_location"] = True

    # Detect District (e.g., Quận 1, Quận Bình Thạnh, etc.)
    dist_match = re.search(r"\b(quận\s+\d+|quận\s+[a-zA-Z\sđàáảãạăắằẳẵặâấầẩẫậêếềểễệôốồổỗộơớờởỡợưứừửữự]+|huyện\s+[a-zA-Z\sđàáảãạăắằẳẵặâấầẩẫậêếềểễệôốồổỗộơớờởỡợưứừửữự]+|thị xã\s+[a-zA-Z\s]+)\b", val)
    if dist_match:
        result["district"] = dist_match.group(0).strip().title()

    # Match province against aliases
    matched_province = None
    for alias, standard_name in DEFAULT_GAZETTEER["province_aliases"].items():
        if alias in val:
            matched_province = standard_name
            break

    # If no alias, test direct containment of standard province names
    if not matched_province:
        for reg, prov_list in DEFAULT_GAZETTEER["regions"].items():
            for p in prov_list:
                if p.lower() in val:
                    matched_province = p
                    break
            if matched_province:
                break

    if matched_province:
        result["province"] = matched_province
        result["region"] = get_region_for_province(matched_province)
    else:
        # Fallback: check if international
        if "singapore" in val:
            result["country"] = "Singapore"
            result["province"] = "Singapore"
        elif "japan" in val or "nhật bản" in val:
            result["country"] = "Japan"
        elif "usa" in val or "mỹ" in val:
            result["country"] = "United States"
        else:
            result["province"] = str(raw_val).strip().title()

    return result
