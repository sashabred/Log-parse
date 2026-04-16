"""
audit_logger.py — Structured JSONL audit trail for each pipeline run.

One JSON object per processed file, written to:
  Logs/audit/run_[TIMESTAMP].txt

Each entry contains all fields specified in the audit schema:
  timestamp, trigger_type, file_name, format, records_processed,
  records_skipped, parsing_method, field_mappings_applied,
  confidence_distribution, errors, metrics_snapshot,
  human_approval_flags
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


class AuditLogger:
    def __init__(self, audit_dir: Path, run_id: str, trigger_type: str = "manual"):
        self._run_id      = run_id
        self._trigger     = trigger_type
        self._path        = audit_dir / f"run_{run_id}.txt"
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._entries: list[dict] = []

    # ── public API ────────────────────────────────────────────────────────────

    def log_file(
        self,
        file_name: str,
        fmt: str,
        fmt_confidence: float,
        records_processed: int,
        records_skipped: int,
        records_rejected: int,
        parsing_method: str,
        field_mappings: list[str],
        valid_records: list[dict],
        errors: list[str],
        human_flags: list[str],
        anomalies: list[dict],
    ) -> None:
        conf_dist = _confidence_distribution(valid_records)
        metrics   = _metrics_snapshot(valid_records)

        entry = {
            "timestamp":               _now(),
            "run_id":                  self._run_id,
            "trigger_type":            self._trigger,
            "file_name":               file_name,
            "format":                  fmt,
            "format_confidence":       round(fmt_confidence, 3),
            "records_processed":       records_processed,
            "records_skipped":         records_skipped,
            "records_rejected":        records_rejected,
            "parsing_method":          parsing_method,
            "field_mappings_applied":  field_mappings,
            "confidence_distribution": conf_dist,
            "errors":                  errors,
            "metrics_snapshot":        metrics,
            "human_approval_flags":    human_flags,
            "anomalies_detected":      [
                {"detector": a["detector"], "severity": a["severity"], "message": a["message"]}
                for a in anomalies
            ],
        }
        self._entries.append(entry)
        self._flush(entry)

    def log_summary(self, total_records: int, total_files: int, elapsed: float) -> None:
        summary = {
            "timestamp":     _now(),
            "run_id":        self._run_id,
            "event":         "RUN_COMPLETE",
            "total_records": total_records,
            "total_files":   total_files,
            "elapsed_sec":   round(elapsed, 3),
        }
        self._flush(summary)

    def log_error(self, context: str, error: str) -> None:
        entry = {
            "timestamp": _now(),
            "run_id":    self._run_id,
            "event":     "PIPELINE_ERROR",
            "context":   context,
            "error":     error,
        }
        self._flush(entry)

    @property
    def path(self) -> Path:
        return self._path

    # ── private ───────────────────────────────────────────────────────────────

    def _flush(self, entry: dict) -> None:
        with self._path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")


# ── helpers ───────────────────────────────────────────────────────────────────

def _confidence_distribution(records: list[dict]) -> dict[str, int]:
    dist = {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "CRITICAL": 0}
    for r in records:
        c = r.get("confidence", 0)
        if c >= 0.85:
            dist["HIGH"] += 1
        elif c >= 0.70:
            dist["MEDIUM"] += 1
        elif c >= 0.50:
            dist["LOW"] += 1
        else:
            dist["CRITICAL"] += 1
    return dist


def _metrics_snapshot(records: list[dict]) -> dict:
    if not records:
        return {}
    total = len(records)
    success = sum(1 for r in records if r.get("status") == "SUCCESS")
    lats = [r["latency_ms"] for r in records if r.get("latency_ms") is not None]
    vols = [r["volume"]     for r in records if r.get("volume")     is not None]
    return {
        "total_ops":          total,
        "success_rate_%":     round(success / total * 100, 2) if total else 0,
        "avg_latency_ms":     round(sum(lats) / len(lats), 2) if lats else None,
        "total_volume":       round(sum(vols), 2) if vols else None,
    }


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
