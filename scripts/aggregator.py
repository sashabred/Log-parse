"""
aggregator.py — KPI computation from normalised withdrawal records.

Groups by (psp, bank, country) → one metrics row per group.
Also provides aggregate_by_psp() for top-level PSP summary.

Metrics computed
----------------
psp, bank, country
total_ops, success_ops, failed_ops, decline_ops, error_count
success_rate_%, decline_rate_%, error_rate_%, conversion_rate_%
total_withdrawals_checked
job_starts, job_finishes, job_completion_rate_%
avg_latency_ms, p50_latency_ms, p95_latency_ms, max_latency_ms
transaction_total_volume

Definitions
-----------
success_ops      : status == SUCCESS
decline_ops      : error == DECLINED
error_count      : FAILED where error NOT IN {DECLINED, ""}  (technical errors)
failed_ops       : total - success_ops  (includes declines + technical errors)
job_starts       : count of request / job_start events (or total if none)
job_finishes     : job_starts - timeout_count  (TIMEOUT = no definitive response)
conversion_rate_%: success_rate_%  (standard payout metric)
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from typing import Any

_DECLINE_CODES  = {"DECLINED"}
_TIMEOUT_CODES  = {"TIMEOUT"}
_TECHNICAL_ERR  = {"NETWORK_ERROR", "TIMEOUT", "INSUFFICIENT_FUNDS"}


# ── public API ────────────────────────────────────────────────────────────────

def aggregate(records: list[dict]) -> list[dict]:
    """Aggregate by (psp, bank, country) → sorted list of metric dicts."""
    buckets: dict[tuple, list[dict]] = defaultdict(list)
    for rec in records:
        key = (
            _str(rec.get("psp")),
            _str(rec.get("bank")),
            _str(rec.get("country")),
        )
        buckets[key].append(rec)

    return [
        _metrics(psp, bank, country, group)
        for (psp, bank, country), group in sorted(buckets.items())
    ]


def aggregate_by_psp(records: list[dict]) -> list[dict]:
    """Aggregate by psp only — for high-level PSP summary table."""
    buckets: dict[str, list[dict]] = defaultdict(list)
    for rec in records:
        buckets[_str(rec.get("psp"))].append(rec)
    return [
        _metrics(psp, "ALL", "ALL", group)
        for psp, group in sorted(buckets.items())
    ]


# ── computation ───────────────────────────────────────────────────────────────

def _metrics(psp: str, bank: str, country: str, group: list[dict]) -> dict:
    total_ops = len(group)

    statuses    = [_str(r.get("status")) for r in group]
    errors      = [_str(r.get("error")).upper()  for r in group]
    event_types = [_str(r.get("event_type")).lower() for r in group]

    success_ops = sum(1 for s in statuses if s == "SUCCESS")
    decline_ops = sum(1 for e in errors   if e in _DECLINE_CODES)
    error_count = sum(
        1 for s, e in zip(statuses, errors)
        if s != "SUCCESS" and e in _TECHNICAL_ERR
    )
    failed_ops  = total_ops - success_ops

    job_starts   = sum(1 for et in event_types if et in {"request", "job_start"}) or total_ops
    timeout_cnt  = sum(1 for e in errors if e in _TIMEOUT_CODES)
    job_finishes = job_starts - timeout_cnt

    latencies = [r["latency_ms"] for r in group if r.get("latency_ms") is not None]
    avg_lat   = round(statistics.mean(latencies), 2) if latencies else 0.0
    p50_lat   = _pct_rank(latencies, 50)
    p95_lat   = _pct_rank(latencies, 95)
    max_lat   = round(max(latencies), 2) if latencies else 0.0

    volumes     = [r["volume"] for r in group if r.get("volume") is not None]
    total_vol   = round(sum(volumes), 2) if volumes else 0.0

    return {
        "psp":                       psp,
        "bank":                      bank,
        "country":                   country,
        "total_ops":                 total_ops,
        "success_ops":               success_ops,
        "failed_ops":                failed_ops,
        "decline_ops":               decline_ops,
        "error_count":               error_count,
        "success_rate_%":            _pct(success_ops,  total_ops),
        "decline_rate_%":            _pct(decline_ops,  total_ops),
        "error_rate_%":              _pct(error_count,  total_ops),
        "conversion_rate_%":         _pct(success_ops,  total_ops),
        "total_withdrawals_checked": total_ops,
        "job_starts":                job_starts,
        "job_finishes":              job_finishes,
        "job_completion_rate_%":     _pct(job_finishes, job_starts),
        "avg_latency_ms":            avg_lat,
        "p50_latency_ms":            p50_lat,
        "p95_latency_ms":            p95_lat,
        "max_latency_ms":            max_lat,
        "transaction_total_volume":  total_vol,
    }


# ── helpers ───────────────────────────────────────────────────────────────────

def _pct(num: int | float, den: int | float, dp: int = 2) -> float:
    return round(num / den * 100, dp) if den else 0.0


def _pct_rank(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    sv  = sorted(values)
    idx = max(0, int(len(sv) * p / 100) - 1)
    return round(sv[idx], 2)


def _str(v: Any) -> str:
    return str(v or "UNKNOWN").strip() or "UNKNOWN"
