"""
normalizer.py — Unified schema mapping, validation, confidence scoring,
and severity classification.

Unified output schema (all keys always present)
------------------------------------------------
id              str     Transaction identifier
guid            str     Correlation / trace ID
request         str     HTTP verb + path
request_body    str     JSON-serialised request payload
timestamp       str     ISO-8601 datetime string
response        str     Response body (if captured)
latency_ms      float   Round-trip latency in ms  (None if unavailable)
volume          float   Monetary amount            (None if unavailable)
psp             str     Payment service provider
bank            str     Bank / issuer name
country         str     ISO country code (upper)
status          str     SUCCESS | FAILED | DECLINED | UNKNOWN
error           str     Error / decline reason code
event_type      str     request | response | job_start | job_finish | unknown
source_file     str     Origin filename
source_format   str     Detected format ID
confidence      float   0.0 – 1.0
severity        str     CRITICAL | HIGH | MEDIUM | LOW | INFO

Validation rules
----------------
- Required fields: psp, status, timestamp
- timestamp must be parseable → normalised to ISO-8601
- status must be in {SUCCESS, FAILED, DECLINED, UNKNOWN}
- latency_ms: 0 < value < 300,000  (else null + flagged)
- volume:     0 < value < 1,000,000 (else null + flagged)
- Duplicate id within a source file → keep first, mark duplicate in field
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

# ── constants ─────────────────────────────────────────────────────────────────

REQUIRED_FIELDS = ("psp", "status", "timestamp")
CORE_FIELDS     = ("psp", "bank", "country", "status", "volume", "latency_ms")

VALID_STATUSES  = {"SUCCESS", "FAILED", "DECLINED", "UNKNOWN"}

LATENCY_MIN, LATENCY_MAX = 0, 300_000
VOLUME_MIN,  VOLUME_MAX  = 0, 1_000_000

_TS_FORMATS = [
    "%m/%d/%Y, %H:%M:%S.%f",   # Azure: 04/16/2026, 05:06:02.000000
    "%Y/%m/%d %H:%M:%S",        # test:  2026/04/16 13:20:05
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S,%f",
    "%Y-%m-%d %H:%M:%S",
    "%d/%b/%Y:%H:%M:%S",
]

_ERROR_SEVERITY: dict[str, str] = {
    "NETWORK_ERROR":      "CRITICAL",
    "TIMEOUT":            "HIGH",
    "DECLINED":           "MEDIUM",
    "INSUFFICIENT_FUNDS": "LOW",
}

_STATUS_SEVERITY: dict[str, str] = {
    "SUCCESS":  "INFO",
    "FAILED":   "HIGH",
    "DECLINED": "MEDIUM",
    "UNKNOWN":  "HIGH",
}


# ── public API ────────────────────────────────────────────────────────────────

def normalize(raw: dict, format_confidence: float = 1.0) -> dict:
    """
    Map *raw* (any format-specific dict) to the unified schema.
    Returns a fully populated dict including validation flags.
    """
    flags: list[str] = []

    # ── extract core values ───────────────────────────────────────────────────
    status  = _norm_status(raw.get("status"), flags)
    error   = str(raw.get("error") or "").upper().strip()
    psp     = str(raw.get("psp") or "").strip()
    bank    = str(raw.get("bank") or "").strip()
    country = str(raw.get("country") or "").strip().upper()

    latency = _valid_latency(
        raw.get("latency_ms") or raw.get("latency") or raw.get("duration_ms"),
        flags,
    )
    volume = _valid_volume(
        raw.get("amount") or raw.get("volume") or raw.get("transaction_amount"),
        flags,
    )
    timestamp = _parse_ts(
        raw.get("timestamp") or raw.get("time") or raw.get("@timestamp"),
        flags,
    )

    # ── validate required fields ──────────────────────────────────────────────
    if not psp:
        flags.append("MISSING_PSP")
    if not timestamp:
        flags.append("MISSING_TIMESTAMP")

    # ── logical status (DECLINED is a sub-type of FAILED) ────────────────────
    logical_status = "DECLINED" if (status == "FAILED" and error == "DECLINED") else status

    # ── build record ──────────────────────────────────────────────────────────
    record: dict = {
        "id":            str(raw.get("id") or raw.get("tx_id") or raw.get("transaction_id") or ""),
        "guid":          str(raw.get("guid") or raw.get("correlation_id") or ""),
        "request":       str(raw.get("request") or "POST /withdrawals/process"),
        "request_body":  str(raw.get("request_body") or ""),
        "timestamp":     timestamp,
        "response":      str(raw.get("response") or raw.get("response_body") or ""),
        "latency_ms":    latency,
        "volume":        volume,
        "psp":           psp,
        "bank":          bank,
        "country":       country,
        "status":        logical_status,
        "error":         error,
        "event_type":    str(raw.get("event_type") or "request").lower(),
        "source_file":   str(raw.get("source_file") or ""),
        "source_format": str(raw.get("source_format") or ""),
        "validation_flags": "|".join(flags) if flags else "",
        "is_valid":      len([f for f in flags if "MISSING" in f]) == 0,
    }

    record["confidence"] = _confidence(record, format_confidence)
    record["severity"]   = _severity(logical_status, error)
    return record


def normalize_batch(records: list[dict], format_confidence: float = 1.0) -> tuple[list[dict], list[dict]]:
    """
    Normalize a batch of raw records.

    Returns
    -------
    (valid_records, rejected_records)
    """
    seen_ids: set[str] = set()
    valid:    list[dict] = []
    rejected: list[dict] = []

    for raw in records:
        try:
            rec = normalize(raw, format_confidence)
        except Exception as exc:
            raw["rejection_reason"] = f"NORMALIZE_ERROR: {exc}"
            rejected.append(raw)
            continue

        # Duplicate id check within batch
        rec_id = rec["id"]
        if rec_id and rec_id in seen_ids:
            rec["validation_flags"] = (rec["validation_flags"] + "|DUPLICATE_ID").lstrip("|")
            rec["is_valid"] = False

        if rec_id:
            seen_ids.add(rec_id)

        if rec["is_valid"]:
            valid.append(rec)
        else:
            rec["rejection_reason"] = rec["validation_flags"]
            rejected.append(rec)

    return valid, rejected


# ── helpers ───────────────────────────────────────────────────────────────────

def _norm_status(raw: Any, flags: list[str]) -> str:
    s = str(raw or "").upper().strip()
    if s in VALID_STATUSES:
        return s
    if s in {"TRUE", "OK", "200", "COMPLETED"}:
        return "SUCCESS"
    if s in {"FALSE", "ERROR", "FAIL", "KO", "500", "4XX"}:
        return "FAILED"
    if s in {"DECLINE", "DECLINED"}:
        return "DECLINED"
    if s:
        flags.append(f"INVALID_STATUS:{s}")
    return "UNKNOWN"


def _parse_ts(raw: Any, flags: list[str]) -> str:
    if not raw:
        return ""
    s = str(raw).strip()
    for fmt in _TS_FORMATS:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    flags.append("UNPARSEABLE_TIMESTAMP")
    return s   # return as-is rather than losing the data


def _valid_latency(raw: Any, flags: list[str]) -> float | None:
    v = _to_float(raw)
    if v is None:
        return None
    if not (LATENCY_MIN < v < LATENCY_MAX):
        flags.append(f"LATENCY_OUT_OF_RANGE:{v}")
        return None
    return v


def _valid_volume(raw: Any, flags: list[str]) -> float | None:
    v = _to_float(raw)
    if v is None:
        return None
    if not (VOLUME_MIN < v < VOLUME_MAX):
        flags.append(f"VOLUME_OUT_OF_RANGE:{v}")
        return None
    return v


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return None


def _confidence(record: dict, fmt_conf: float) -> float:
    present = sum(
        1 for f in CORE_FIELDS
        if record.get(f) not in (None, "", "N/A", "null", "UNKNOWN")
    )
    coverage = present / len(CORE_FIELDS)
    # source quality: penalise adaptive / partially matched records
    src_quality = {
        "azure_withdrawal_csv": 1.00,
        "freetext_numbered":    1.00,
        "json_lines":           0.95,
        "json_array":           0.95,
        "csv_generic":          0.85,
        "adaptive_unknown":     0.65,
    }.get(record.get("source_format", ""), 0.70)

    score = coverage * fmt_conf * src_quality
    return round(min(max(score, 0.0), 1.0), 3)


def _severity(status: str, error: str) -> str:
    if error in _ERROR_SEVERITY:
        return _ERROR_SEVERITY[error]
    return _STATUS_SEVERITY.get(status, "INFO")
