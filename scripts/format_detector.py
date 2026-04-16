"""
format_detector.py — Two-stage format detection.

Stage A: Extension pre-filter  (.xlsx, .jsonl → fast-path)
Stage B: Structural / content sniffing of first 25 lines

Returns (LogFormat, confidence_float).
Confidence < 0.70 triggers DECISION_REQUEST in the orchestrator.
"""

from __future__ import annotations

import csv
import io
import json
import re
from enum import Enum
from pathlib import Path


class LogFormat(Enum):
    AZURE_WITHDRAWAL_CSV = "azure_withdrawal_csv"
    FREETEXT_NUMBERED    = "freetext_numbered"
    JSON_LINES           = "json_lines"
    JSON_ARRAY           = "json_array"
    CSV_GENERIC          = "csv_generic"
    XLSX                 = "xlsx"
    UNKNOWN              = "unknown"


SAMPLE_LINES = 25

# ── heuristic patterns ────────────────────────────────────────────────────────

# azure: quoted timestamp + tx-id + quoted POST path + ... + JSON body with ""psp""
_RE_AZURE = re.compile(
    r'^"[^"]{10,30}",\s*\w+-\d+,\s*"[^"]+",.*\{.*(?:""psp""|"psp")',
    re.IGNORECASE,
)

# freetext: "N. YYYY/MM/DD HH:MM:SS - Withdrawal processed/failed -"
_RE_FREETEXT = re.compile(
    r"^\s*\d+\.\s+\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}\s+-\s+withdrawal",
    re.IGNORECASE,
)

_RE_JSON_OBJ  = re.compile(r"^\s*\{.*\}\s*$")
_RE_JSON_ARR  = re.compile(r"^\s*\[")


# ── public API ────────────────────────────────────────────────────────────────

def detect(file_path: str | Path, sample_lines: int = SAMPLE_LINES) -> tuple[LogFormat, float]:
    """
    Detect the format of *file_path*.

    Returns
    -------
    (LogFormat, confidence)  where confidence is in [0.0, 1.0]
    """
    path = Path(file_path)
    ext  = path.suffix.lower()

    # Stage A — extension fast-paths
    if ext == ".xlsx":
        return LogFormat.XLSX, 1.0
    if ext == ".jsonl":
        return LogFormat.JSON_LINES, 0.95

    # Stage B — structural sniffing
    lines = _sample(path, sample_lines)
    non_empty = [ln for ln in lines if ln.strip()]
    if not non_empty:
        return LogFormat.UNKNOWN, 0.0

    # JSON array
    if _RE_JSON_ARR.match(non_empty[0]):
        return LogFormat.JSON_ARRAY, 0.95

    # JSON lines — need ≥80% of sample lines to be valid JSON objects
    json_hits = sum(1 for ln in non_empty if _RE_JSON_OBJ.match(ln))
    if json_hits / len(non_empty) >= 0.80:
        return LogFormat.JSON_LINES, _conf(0.70, json_hits, len(non_empty))

    # Azure withdrawal CSV
    azure_hits = sum(1 for ln in non_empty if _RE_AZURE.match(ln))
    if azure_hits / len(non_empty) >= 0.60:
        return LogFormat.AZURE_WITHDRAWAL_CSV, _conf(0.70, azure_hits, len(non_empty))

    # Freetext numbered
    ft_hits = sum(1 for ln in non_empty if _RE_FREETEXT.match(ln))
    if ft_hits / len(non_empty) >= 0.40:
        return LogFormat.FREETEXT_NUMBERED, _conf(0.60, ft_hits, len(non_empty))

    # Generic CSV (has a parseable header)
    if _is_csv_header(non_empty[0]):
        return LogFormat.CSV_GENERIC, 0.70

    return LogFormat.UNKNOWN, 0.30


def detect_all(log_dir: str | Path) -> dict[Path, tuple[LogFormat, float]]:
    """Detect format for every supported file in *log_dir*."""
    supported = {".txt", ".csv", ".json", ".jsonl", ".log", ".xlsx"}
    results: dict[Path, tuple[LogFormat, float]] = {}
    for fp in sorted(Path(log_dir).iterdir()):
        if fp.is_file() and fp.suffix.lower() in supported:
            results[fp] = detect(fp)
    return results


# ── helpers ───────────────────────────────────────────────────────────────────

def _sample(path: Path, n: int) -> list[str]:
    try:
        with path.open(encoding="utf-8", errors="replace") as fh:
            return [fh.readline() for _ in range(n)]
    except OSError:
        return []


def _conf(base: float, hits: int, total: int) -> float:
    """Scale confidence between base and 1.0 proportional to hit rate."""
    rate = hits / total if total else 0
    return round(min(base + rate * (1.0 - base), 1.0), 3)


def _is_csv_header(line: str) -> bool:
    try:
        cols = next(csv.reader(io.StringIO(line)))
        return len(cols) >= 3 and all(len(c) < 60 for c in cols)
    except Exception:
        return False
