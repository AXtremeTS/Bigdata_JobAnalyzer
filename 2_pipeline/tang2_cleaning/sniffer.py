# etl/jobmarket_etl/sniffer.py
"""
Sniffer module to automatically detect CSV format parameters:
- Encoding (utf-8, utf-8-sig, cp1258, latin-1)
- Delimiter (comma, semicolon, tab, pipe)
- Header presence
- Multiline / Quote status
"""

import csv
import re
from typing import Dict, Any, List

COMMON_ENCODINGS = ["utf-8-sig", "utf-8", "cp1258", "latin-1"]
COMMON_DELIMITERS = [",", ";", "\t", "|"]


def sniff_encoding(sample_bytes: bytes) -> str:
    """Detects encoding by trying candidates in priority order."""
    if sample_bytes.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    if sample_bytes.startswith(b"\xff\xfe") or sample_bytes.startswith(b"\xfe\xff"):
        return "utf-16"
        
    for enc in COMMON_ENCODINGS:
        try:
            sample_bytes.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "utf-8"


def sniff_delimiter(text_sample: str) -> str:
    """Detects delimiter by inspecting lines using csv.Sniffer and heuristic counting."""
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(text_sample[:10000], delimiters=";,|\t")
        return dialect.delimiter
    except Exception:
        # Fallback to frequency count on first non-empty lines
        lines = [line for line in text_sample.splitlines() if line.strip()][:10]
        if not lines:
            return ","
        
        counts = {d: 0 for d in COMMON_DELIMITERS}
        for d in COMMON_DELIMITERS:
            counts[d] = sum(line.count(d) for line in lines)
        best_delimiter = max(counts, key=counts.get)
        return best_delimiter if counts[best_delimiter] > 0 else ","


def normalize_column_name(col: str) -> str:
    """Normalizes raw column header to standard snake_case identifier."""
    c = col.strip().lower()
    # Replace non-alphanumeric with underscores
    c = re.sub(r"[^\w\s]", "", c)
    c = re.sub(r"\s+", "_", c)
    return c if c else "col"


def sniff_csv_format(file_path: str, sample_size: int = 65536) -> Dict[str, Any]:
    """Inspects a CSV file and returns dialect parameters for PySpark reader."""
    with open(file_path, "rb") as f:
        raw_sample = f.read(sample_size)

    encoding = sniff_encoding(raw_sample)
    decoded_sample = raw_sample.decode(encoding, errors="replace")

    delimiter = sniff_delimiter(decoded_sample)

    # Detect header and quotes
    has_header = True
    try:
        sniffer = csv.Sniffer()
        has_header = sniffer.has_header(decoded_sample[:10000])
    except Exception:
        has_header = True

    lines = [l for l in decoded_sample.splitlines() if l.strip()]
    # If sniffer returned False, verify if line 0 looks like an alphanumeric header
    if not has_header and lines:
        first_tokens = lines[0].split(delimiter)
        if any(re.search(r"[a-zA-Z_\u00C0-\u1EF9]", t) for t in first_tokens if t.strip()):
            has_header = True

    # Check for multiline fields (newlines inside quotes)
    quote_count = decoded_sample.count('"')
    is_multiline = quote_count >= 2

    # Extract columns if header is present
    columns = []
    if has_header and lines:
        try:
            row_reader = csv.reader([lines[0]], delimiter=delimiter)
            columns = [c.strip() for c in next(row_reader, [])]
        except Exception:
            columns = [c.strip() for c in lines[0].split(delimiter)]

    return {
        "encoding": encoding,
        "delimiter": delimiter,
        "has_header": has_header,
        "is_multiline": is_multiline,
        "columns": columns,
        "quote_char": '"',
        "escape_char": "\\"
    }
