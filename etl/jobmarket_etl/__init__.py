# etl/jobmarket_etl/__init__.py
"""
jobmarket_etl core library for adaptive schema ingestion, normalization,
quality evaluation, and capability detection.
"""

from .sniffer import sniff_csv_format, normalize_column_name
from .mapper import map_column, generate_schema_mapping, CANONICAL_FIELDS
from .market_detector import detect_market
from .quality import evaluate_quality_metrics
from .capabilities import build_capabilities

__all__ = [
    "sniff_csv_format",
    "normalize_column_name",
    "map_column",
    "generate_schema_mapping",
    "CANONICAL_FIELDS",
    "detect_market",
    "evaluate_quality_metrics",
    "build_capabilities"
]
