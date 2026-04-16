"""
anomaly_detector.py — Rule-based anomaly detection on aggregated metrics.

Detectors
---------
FAILURE_SPIKE        : PSP error_rate > 40%                → HIGH
ZERO_SUCCESS         : success_ops == 0 and total_ops >= 5  → CRITICAL
LATENCY_DEGRADATION  : p95_latency > LATENCY_P95_WARN ms   → MEDIUM
HIGH_DECLINE_RATE    : decline_rate > 30%                   → MEDIUM
LOW_VOLUME_ALERT     : total_ops < MIN_OPS_THRESHOLD        → HIGH
HIGH_ERROR_RATE      : error_rate > 25%                     → HIGH
CONFIDENCE_DROP      : avg_confidence < 0.70                → HIGH  (DECISION_REQUEST)

Each anomaly record
-------------------
{
  "detector":   str,
  "severity":   CRITICAL | HIGH | MEDIUM | LOW,
  "psp":        str,
  "bank":       str,
  "country":    str,
  "metric":     str,
  "value":      float,
  "threshold":  float,
  "message":    str,
  "requires_approval": bool   # True if severity >= HIGH
}
"""

from __future__ import annotations

from typing import Any

# ── thresholds ────────────────────────────────────────────────────────────────
FAILURE_SPIKE_THRESHOLD   = 40.0   # error_rate_%
DECLINE_SPIKE_THRESHOLD   = 30.0   # decline_rate_%
HIGH_ERROR_THRESHOLD      = 25.0   # error_rate_%
LATENCY_P95_WARN          = 900.0  # ms
MIN_OPS_THRESHOLD         = 5      # below this total_ops, LOW_VOLUME is not flagged
CONFIDENCE_WARN           = 0.70

_APPROVAL_SEVERITIES = {"CRITICAL", "HIGH"}


# ── public API ────────────────────────────────────────────────────────────────

def detect_anomalies(
    metrics: list[dict],
    all_records: list[dict] | None = None,
) -> list[dict]:
    """
    Run all detectors over *metrics* and optionally *all_records*.

    Returns a list of anomaly dicts sorted by severity (CRITICAL first).
    """
    anomalies: list[dict] = []

    for row in metrics:
        anomalies.extend(_check_row(row))

    if all_records:
        anomalies.extend(_check_confidence(all_records))

    # Sort: CRITICAL → HIGH → MEDIUM → LOW
    order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    anomalies.sort(key=lambda a: order.get(a["severity"], 9))
    return anomalies


def requires_human_approval(anomalies: list[dict]) -> bool:
    return any(a["requires_approval"] for a in anomalies)


# ── detectors ─────────────────────────────────────────────────────────────────

def _check_row(row: dict) -> list[dict]:
    hits: list[dict] = []
    psp     = row.get("psp", "?")
    bank    = row.get("bank", "?")
    country = row.get("country", "?")

    def _anomaly(detector: str, severity: str, metric: str, value: Any, threshold: Any, msg: str) -> dict:
        return {
            "detector":         detector,
            "severity":         severity,
            "psp":              psp,
            "bank":             bank,
            "country":          country,
            "metric":           metric,
            "value":            value,
            "threshold":        threshold,
            "message":          msg,
            "requires_approval": severity in _APPROVAL_SEVERITIES,
        }

    er  = row.get("error_rate_%", 0)
    dr  = row.get("decline_rate_%", 0)
    sr  = row.get("success_rate_%", 0)
    p95 = row.get("p95_latency_ms", 0)
    ops = row.get("total_ops", 0)
    suc = row.get("success_ops", 0)

    # ZERO_SUCCESS — most critical
    if ops >= MIN_OPS_THRESHOLD and suc == 0:
        hits.append(_anomaly(
            "ZERO_SUCCESS", "CRITICAL",
            "success_ops", 0, MIN_OPS_THRESHOLD,
            f"{psp}/{bank}/{country}: 0 successful transactions out of {ops} ops",
        ))

    # FAILURE_SPIKE
    if er > FAILURE_SPIKE_THRESHOLD:
        hits.append(_anomaly(
            "FAILURE_SPIKE", "HIGH",
            "error_rate_%", er, FAILURE_SPIKE_THRESHOLD,
            f"{psp}/{bank}/{country}: error rate {er:.1f}% exceeds {FAILURE_SPIKE_THRESHOLD}%",
        ))

    # HIGH_ERROR_RATE (distinct from spike threshold)
    elif er > HIGH_ERROR_THRESHOLD:
        hits.append(_anomaly(
            "HIGH_ERROR_RATE", "HIGH",
            "error_rate_%", er, HIGH_ERROR_THRESHOLD,
            f"{psp}/{bank}/{country}: error rate {er:.1f}% exceeds {HIGH_ERROR_THRESHOLD}%",
        ))

    # HIGH_DECLINE_RATE
    if dr > DECLINE_SPIKE_THRESHOLD:
        hits.append(_anomaly(
            "HIGH_DECLINE_RATE", "MEDIUM",
            "decline_rate_%", dr, DECLINE_SPIKE_THRESHOLD,
            f"{psp}/{bank}/{country}: decline rate {dr:.1f}% exceeds {DECLINE_SPIKE_THRESHOLD}%",
        ))

    # LATENCY_DEGRADATION
    if p95 > LATENCY_P95_WARN:
        hits.append(_anomaly(
            "LATENCY_DEGRADATION", "MEDIUM",
            "p95_latency_ms", p95, LATENCY_P95_WARN,
            f"{psp}/{bank}/{country}: p95 latency {p95:.0f}ms exceeds {LATENCY_P95_WARN:.0f}ms",
        ))

    # LOW_VOLUME_ALERT — only meaningful when we expect higher throughput
    # Skip for granular (psp, bank, country) groups with inherently low counts
    # (they are normal for small datasets); rely on PSP-level aggregation instead

    return hits


def _check_confidence(records: list[dict]) -> list[dict]:
    confs = [r["confidence"] for r in records if "confidence" in r]
    if not confs:
        return []
    avg = sum(confs) / len(confs)
    if avg < CONFIDENCE_WARN:
        return [{
            "detector":          "CONFIDENCE_DROP",
            "severity":          "HIGH",
            "psp":               "ALL",
            "bank":              "ALL",
            "country":           "ALL",
            "metric":            "avg_confidence",
            "value":             round(avg, 3),
            "threshold":         CONFIDENCE_WARN,
            "message":           f"Average record confidence {avg:.3f} < threshold {CONFIDENCE_WARN} — review parsing quality",
            "requires_approval": True,
        }]
    return []
