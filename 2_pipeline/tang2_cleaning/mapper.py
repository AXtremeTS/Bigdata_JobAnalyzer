# etl/jobmarket_etl/mapper.py
"""
Semantic Column Mapper module:
Maps arbitrary incoming CSV headers to Canonical Schema fields using:
1. Exact & Fuzzy matching against multilingual aliases dictionary.
2. Content profiling heuristics (values matching currency, locations, dates, skills).
"""

import os
import yaml
import re
from typing import Dict, List, Any, Optional
from difflib import SequenceMatcher

CANONICAL_FIELDS = [
    "job_title",
    "company",
    "salary",
    "location",
    "country",
    "experience",
    "education",
    "technical_skills",
    "soft_skills",
    "contract_type",
    "description",
    "requirements",
    "category",
    "posted_date"
]


def load_aliases(reference_dir: Optional[str] = None) -> Dict[str, List[str]]:
    """Loads column aliases dictionary from YAML."""
    if not reference_dir:
        # Resolve relative to project root
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        reference_dir = os.path.join(base_dir, "reference")
    
    alias_path = os.path.join(reference_dir, "column_aliases.yml")
    if os.path.exists(alias_path):
        with open(alias_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return data.get("aliases", {})
    return {}


def similarity(s1: str, s2: str) -> float:
    """Calculates string similarity score between 0.0 and 1.0."""
    return SequenceMatcher(None, s1.lower().strip(), s2.lower().strip()).ratio()


def map_column(col_name: str, sample_values: List[str], aliases: Dict[str, List[str]]) -> Dict[str, Any]:
    """
    Determines the best canonical target field for a given column header.
    Returns: { "target": str, "confidence": float, "reason": str }
    """
    cleaned_col = col_name.lower().strip().replace("_", " ")
    
    best_target = None
    best_score = 0.0
    reason = "No matching alias or profile pattern"

    # 1. Header Name Matching against Aliases
    col_words = set(cleaned_col.split())
    for target_field, alias_list in aliases.items():
        for alias in alias_list:
            alias_clean = alias.lower().strip().replace("_", " ")
            if cleaned_col == alias_clean:
                return {
                    "target": target_field,
                    "confidence": 1.0,
                    "reason": f"Exact alias match with '{alias}'"
                }
            if alias_clean in col_words or alias_clean in cleaned_col:
                if 0.90 > best_score:
                    best_score = 0.90
                    best_target = target_field
                    reason = f"Keyword match with alias '{alias}'"
            sim = similarity(cleaned_col, alias_clean)
            if sim > best_score and sim >= 0.70:
                best_score = sim
                best_target = target_field
                reason = f"Fuzzy header match with '{alias}' (score={round(sim, 2)})"

    # 2. Content Heuristics Validation
    non_null_samples = [str(v).strip() for v in sample_values if v and str(v).lower() not in ("none", "null", "nan", "")]
    if non_null_samples:
        content_sample_str = " ".join(non_null_samples[:20]).lower()
        
        # Salary signals (including dollar sign and salary keywords)
        if re.search(r"(\$|\b(triệu|vnd|usd|lương|negotiable|thoả thuận|/tháng|/năm|per annum|/yr|/year|/hr|a year)\b)", content_sample_str):
            if best_target != "salary":
                best_target = "salary"
                best_score = max(best_score, 0.88)
                reason = "Content heuristic: detected salary patterns, symbols or currencies"
        
        # Location signals
        elif re.search(r"\b(hà nội|hồ chí minh|đà nẵng|quận|huyện|vietnam|việt nam|tphcm|ca|ny|tx|wa|remote)\b", content_sample_str):
            if best_target != "location" and "job_title" not in col_name:
                best_target = "location"
                best_score = max(best_score, 0.80)
                reason = "Content heuristic: detected geographic location names"

        # Skills list signals
        elif "skill" in cleaned_col or "kỹ năng" in cleaned_col:
            best_target = "technical_skills"
            best_score = max(best_score, 0.92)
            reason = "Content heuristic: skill header identifier"

    if best_score >= 0.65 and best_target:
        return {
            "target": best_target,
            "confidence": round(best_score, 2),
            "reason": reason
        }

    return {
        "target": None,
        "confidence": 0.0,
        "reason": f"Unmapped field '{col_name}'"
    }


def generate_schema_mapping(columns: List[str], samples_by_col: Dict[str, List[str]]) -> Dict[str, Any]:
    """Generates complete mapping metadata dictionary for an input dataset."""
    aliases = load_aliases()
    mapping = {}
    mapped_targets = set()

    for col in columns:
        samples = samples_by_col.get(col, [])
        match_info = map_column(col, samples, aliases)
        target = match_info["target"]

        # Prevent duplicate assignment of high confidence targets
        if target and target in mapped_targets:
            # If already mapped, compare confidence
            existing_col = [c for c, m in mapping.items() if m.get("target") == target][0]
            if match_info["confidence"] > mapping[existing_col]["confidence"]:
                mapping[existing_col]["target"] = None
                mapping[existing_col]["confidence"] = 0.0
                mapping[existing_col]["reason"] = f"Superseded by '{col}'"
                mapped_targets.remove(target)
            else:
                match_info["target"] = None
                match_info["confidence"] = 0.0
                match_info["reason"] = f"Duplicate target '{target}' already assigned to '{existing_col}'"

        if match_info["target"]:
            mapped_targets.add(match_info["target"])

        mapping[col] = match_info

    return mapping
