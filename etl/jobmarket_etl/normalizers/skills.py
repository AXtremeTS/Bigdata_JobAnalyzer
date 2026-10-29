# etl/jobmarket_etl/normalizers/skills.py
"""
Skills Normalizer module:
Parses, cleans, and standardizes skills from:
1. Python list representations e.g. "['Python', 'SQL', 'Git']"
2. Comma / semicolon separated text e.g. "React, Node.js; Docker"
3. Free-form text extraction using a canonical tech skills dictionary
"""

import ast
import re
from typing import List, Optional

CANONICAL_SYNONYMS = {
    "reactjs": "React",
    "react.js": "React",
    "react native": "React Native",
    "nodejs": "Node.js",
    "node.js": "Node.js",
    "vuejs": "Vue.js",
    "vue.js": "Vue.js",
    "angularjs": "Angular",
    "angular": "Angular",
    "pyspark": "Spark",
    "apache spark": "Spark",
    "apache hadoop": "Hadoop",
    "apache kafka": "Kafka",
    "ms excel": "Excel",
    "microsoft excel": "Excel",
    "power bi": "Power BI",
    "powerbi": "Power BI",
    "postgres": "PostgreSQL",
    "postgresql": "PostgreSQL",
    "mssql": "SQL Server",
    "sql server": "SQL Server",
    "aws": "AWS",
    "amazon web services": "AWS",
    "azure": "Azure",
    "gcp": "Google Cloud",
    "google cloud": "Google Cloud",
    "docker": "Docker",
    "k8s": "Kubernetes",
    "kubernetes": "Kubernetes",
    "golang": "Go",
    "c#": "C#",
    "c++": "C++",
    ".net": ".NET",
    "dot net": ".NET",
    "spring boot": "Spring Boot",
    "springboot": "Spring Boot",
    "flutter": "Flutter",
    "tensorflow": "TensorFlow",
    "pytorch": "PyTorch",
    "scikit-learn": "Scikit-Learn",
    "git": "Git",
    "github": "GitHub",
    "gitlab": "GitLab",
    "ci/cd": "CI/CD",
    "linux": "Linux"
}


def clean_skill_token(token: str) -> Optional[str]:
    """Cleans a single skill string token."""
    t = token.strip().strip("'\"[](){}.,")
    if not t or len(t) < 2 or len(t) > 30:
        return None
        
    lower_t = t.lower()
    if lower_t in CANONICAL_SYNONYMS:
        return CANONICAL_SYNONYMS[lower_t]
        
    # Return formatted title case or original if capitalized acronym
    if t.isupper() and len(t) <= 5:
        return t
    return t.title() if len(t) > 3 and not any(c.isupper() for c in t[1:]) else t


def normalize_skills(raw_val: Optional[str]) -> List[str]:
    """
    Parses a raw skill string or list representation and returns a deduplicated list
    of canonical skills.
    """
    if not raw_val or str(raw_val).strip() == "":
        return []

    val = str(raw_val).strip()
    extracted_tokens = []

    # Case 1: Python list representation string e.g. "['Python', 'SQL']"
    if val.startswith("[") and val.endswith("]"):
        try:
            parsed = ast.literal_eval(val)
            if isinstance(parsed, (list, tuple, set)):
                for item in parsed:
                    cleaned = clean_skill_token(str(item))
                    if cleaned:
                        extracted_tokens.append(cleaned)
                if extracted_tokens:
                    return list(dict.fromkeys(extracted_tokens))
        except Exception:
            # Fallback to regex tokenization
            tokens = re.findall(r"['\"]([^'\"]+)['\"]", val)
            if tokens:
                for t in tokens:
                    cleaned = clean_skill_token(t)
                    if cleaned:
                        extracted_tokens.append(cleaned)
                if extracted_tokens:
                    return list(dict.fromkeys(extracted_tokens))

    # Case 2: Comma, semicolon, or bullet separated string
    parts = re.split(r"[,;•\n\r/|]", val)
    for part in parts:
        cleaned = clean_skill_token(part)
        if cleaned:
            extracted_tokens.append(cleaned)

    return list(dict.fromkeys(extracted_tokens))
