"""
run_pipeline.py — Full pipeline orchestrator.

PHASES
------
1 (external) — Plan was shown and approved by human.
2            — Ingestion & Normalization
3            — Analytics & Reporting
4            — Audit & Registry Update

Usage
-----
  python scripts/run_pipeline.py
  python scripts/run_pipeline.py --logs Logs --data data --results results --trigger scheduled
  python scripts/run_pipeline.py --all        # reprocess all files (ignore registry)

Guardrails enforced
-------------------
- Write access: scripts/, data/, results/, Logs/audit/ ONLY
- Source logs are never modified
- Existing output files are never overwritten (versioned by timestamp)
- DECISION_REQUEST: pipeline pauses and prints approval prompt; continues/skips per user input
- Per-file fail-safe: exceptions are caught, logged, pipeline continues
- Schema validation gate before aggregation
- Chunked processing for large files
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

# ── path setup ────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.format_detector   import LogFormat, detect
from scripts.parsers           import AzureCsvParser, FreetextParser, JsonParser, CsvGenericParser
from scripts.adaptive_parser   import AdaptiveParser
from scripts.normalizer        import normalize_batch
from scripts.aggregator        import aggregate, aggregate_by_psp
from scripts.anomaly_detector  import detect_anomalies, requires_human_approval
from scripts.audit_logger      import AuditLogger
from scripts.report_generator  import generate as gen_report
from scripts.registry          import Registry

# ── constants ─────────────────────────────────────────────────────────────────
SUPPORTED_EXTENSIONS = {".txt", ".csv", ".json", ".jsonl", ".log", ".xlsx"}
REJECTION_RATE_ALERT = 0.20   # flag if > 20% of records are rejected

_FORMAT_PARSERS = {
    LogFormat.AZURE_WITHDRAWAL_CSV: AzureCsvParser,
    LogFormat.FREETEXT_NUMBERED:    FreetextParser,
    LogFormat.JSON_LINES:           JsonParser,
    LogFormat.JSON_ARRAY:           JsonParser,
    LogFormat.CSV_GENERIC:          CsvGenericParser,
    LogFormat.UNKNOWN:              None,   # → AdaptiveParser
}


# ── helpers ───────────────────────────────────────────────────────────────────

def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        path.write_text("(no data)\n", encoding="utf-8")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _decision_request(payload: dict) -> str:
    """Print a DECISION_REQUEST block and return the user's choice."""
    print("\n" + "=" * 66)
    print("  DECISION_REQUEST")
    print("=" * 66)
    print(f"  Run ID    : {payload.get('run_id', '?')}")
    print(f"  File      : {payload['file']}")
    print(f"  Reason    : {payload['reason']}")
    print("\n  Sample lines:")
    for ln in payload.get("sample_lines", "").splitlines()[:5]:
        print(f"    {ln}")
    print("\n  Options:")
    for k, v in payload.get("options", {}).items():
        print(f"    [{k}] {v}")
    print("=" * 66)
    try:
        choice = input("  Your choice (A/R/M): ").strip().upper()
    except (EOFError, KeyboardInterrupt):
        choice = "R"
    return choice if choice in {"A", "R", "M"} else "R"


def _anomaly_approval(anomalies: list[dict]) -> str:
    """Print anomaly summary and request approval."""
    print("\n" + "=" * 66)
    print("  DECISION_REQUEST — Anomalies Detected")
    print("=" * 66)
    for a in anomalies:
        if a.get("requires_approval"):
            print(f"  [{a['severity']}] {a['message']}")
    print("\n  Options:")
    print("    [A] APPROVE — accept results and continue")
    print("    [R] REJECT  — discard this run's outputs")
    print("=" * 66)
    try:
        choice = input("  Your choice (A/R): ").strip().upper()
    except (EOFError, KeyboardInterrupt):
        choice = "A"
    return choice if choice in {"A", "R"} else "A"


# ── per-file processing ───────────────────────────────────────────────────────

def process_file(
    fp: Path,
    fmt: LogFormat,
    fmt_conf: float,
    run_id: str,
) -> tuple[list[dict], list[dict], dict]:
    """
    Parse, normalize, and validate one file.

    Returns (valid_records, rejected_records, file_stat_dict)
    """
    stat = {
        "file":    fp.name,
        "format":  fmt.value,
        "conf":    fmt_conf,
        "records": 0,
        "rejected": 0,
        "status":  "OK",
        "parsing_method": "rules_based",
        "field_mappings": [],
        "errors":  [],
        "human_flags": [],
    }

    raw_records: list[dict] = []

    # ── select parser ─────────────────────────────────────────────────────────
    parser_cls = _FORMAT_PARSERS.get(fmt)

    if parser_cls is None or fmt == LogFormat.UNKNOWN:
        stat["parsing_method"] = "adaptive"
        parser = AdaptiveParser(fp, format_confidence=fmt_conf)
    else:
        parser = parser_cls(fp)

    # ── xlsx special handling ─────────────────────────────────────────────────
    if fmt == LogFormat.XLSX:
        stat["parsing_method"] = "adaptive_xlsx"
        raw_records = _parse_xlsx(fp, stat)
    else:
        # Iterate with per-record fail-safe
        for rec in parser.parse():
            # Intercept DECISION_REQUEST sentinels from AdaptiveParser
            if isinstance(rec, dict) and rec.get("__type__") == "DECISION_REQUEST":
                rec["run_id"] = run_id
                choice = _decision_request(rec)
                if choice == "R":
                    stat["status"] = "SKIPPED_BY_USER"
                    stat["human_flags"].append("USER_REJECTED_LOW_CONFIDENCE")
                    return [], [], stat
                elif choice == "A":
                    stat["human_flags"].append("USER_APPROVED_LOW_CONFIDENCE")
                    stat["parsing_method"] = "adaptive_approved"
                    # Re-run with forced confidence
                    forced = AdaptiveParser(fp, format_confidence=0.75)
                    for r2 in forced.parse():
                        if not (isinstance(r2, dict) and r2.get("__type__") == "DECISION_REQUEST"):
                            raw_records.append(r2)
                    break
                # M → user can extend, for now treat as A
                else:
                    stat["human_flags"].append("USER_MODIFIED_MAPPING")
                    stat["parsing_method"] = "adaptive_modified"
            else:
                raw_records.append(rec)

    if not raw_records:
        stat["status"] = "NO_RECORDS"
        return [], [], stat

    # ── normalize + validate ──────────────────────────────────────────────────
    valid, rejected = normalize_batch(raw_records, fmt_conf)

    # Rejection rate check
    total = len(raw_records)
    rej_rate = len(rejected) / total if total else 0
    if rej_rate > REJECTION_RATE_ALERT:
        flag = f"HIGH_REJECTION_RATE:{rej_rate:.1%}"
        stat["human_flags"].append(flag)
        print(f"  [ALERT] {flag} — {fp.name}")

    stat["records"]  = len(valid)
    stat["rejected"] = len(rejected)
    stat["field_mappings"] = _infer_mappings(fmt)
    return valid, rejected, stat


def _parse_xlsx(fp: Path, stat: dict) -> list[dict]:
    try:
        import openpyxl
        wb = openpyxl.load_workbook(fp, read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return []
        header = [str(c).strip().lower() if c else f"col_{i}" for i, c in enumerate(rows[0])]
        return [
            {header[i]: (row[i] if i < len(row) else None) for i in range(len(header))}
            for row in rows[1:]
        ]
    except ImportError:
        stat["errors"].append("openpyxl not installed")
        return []
    except Exception as exc:
        stat["errors"].append(f"XLSX_ERROR: {exc}")
        return []


def _infer_mappings(fmt: LogFormat) -> list[str]:
    return {
        LogFormat.AZURE_WITHDRAWAL_CSV: [
            "col[0]->timestamp", "col[1]->id", "col[2]->request",
            "col[7]->latency_ms", "body.psp->psp", "body.bank->bank",
            "body.country->country", "body.status->status",
            "body.amount->volume", "body.error->error",
        ],
        LogFormat.FREETEXT_NUMBERED: [
            "regex(PSP)->psp", "regex(Bank)->bank", "regex(Country)->country",
            "regex(Status)->status", "regex(Amount)->volume",
            "regex(Latency)->latency_ms", "regex(Reason)->error",
        ],
        LogFormat.JSON_LINES:  ["json_keys->alias_table->unified_schema"],
        LogFormat.JSON_ARRAY:  ["json_keys->alias_table->unified_schema"],
        LogFormat.CSV_GENERIC: ["header_names->alias_table->unified_schema"],
    }.get(fmt, ["adaptive->regex_scan->unified_schema"])


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Withdrawal Log Pipeline")
    parser.add_argument("--logs",    default="Logs",    help="Log source directory")
    parser.add_argument("--data",    default="data",    help="Normalized data output dir")
    parser.add_argument("--results", default="results", help="Reports output dir")
    parser.add_argument("--trigger", default="manual",  help="Trigger type for audit log")
    parser.add_argument("--all",     action="store_true", help="Reprocess all files")
    args = parser.parse_args()

    logs_dir    = _ROOT / args.logs
    data_dir    = _ROOT / args.data / "normalized"
    results_dir = _ROOT / args.results
    audit_dir   = _ROOT / args.logs / "audit"
    scripts_dir = _ROOT / "scripts"

    for d in (data_dir, results_dir, audit_dir):
        d.mkdir(parents=True, exist_ok=True)

    if not logs_dir.exists():
        print(f"[ERROR] Logs directory not found: {logs_dir}")
        sys.exit(1)

    run_id  = _run_id()
    ts      = _ts()
    t0      = time.time()

    registry = Registry(scripts_dir)
    audit    = AuditLogger(audit_dir, run_id, args.trigger)

    print(f"\n{'='*60}")
    print(f"  WITHDRAWAL LOG PIPELINE  |  Run: {run_id}")
    print(f"{'='*60}")

    # ── PHASE 2: Ingestion ────────────────────────────────────────────────────
    print("\n[PHASE 2] Ingestion & Normalization\n")

    files = sorted(
        f for f in logs_dir.iterdir()
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS
        and not f.parent.name == "audit"
    )
    if not files:
        print("[WARN] No supported files found.")
        sys.exit(0)

    all_valid:    list[dict] = []
    all_rejected: list[dict] = []
    file_stats:   list[dict] = []

    for fp in files:
        should, attempt = registry.needs_processing(fp) if not args.all else (True, 1)
        if not should:
            print(f"  [SKIP] {fp.name}  (already processed)")
            continue

        fmt, fmt_conf = detect(fp)
        print(f"  [{fmt.value}  conf={fmt_conf:.2f}  attempt={attempt}]  {fp.name}")

        try:
            valid, rejected, stat = process_file(fp, fmt, fmt_conf, run_id)
        except Exception as exc:
            tb = traceback.format_exc()
            audit.log_error(fp.name, tb)
            stat = {
                "file": fp.name, "format": fmt.value, "conf": fmt_conf,
                "records": 0, "rejected": 0, "status": "ERROR",
                "parsing_method": "error", "field_mappings": [],
                "errors": [str(exc)], "human_flags": [],
            }
            registry.record({
                "file_name": fp.name, "sha256": registry.sha256(fp),
                "mtime": str(fp.stat().st_mtime), "format": fmt.value,
                "records_total": 0, "records_extracted": 0,
                "records_skipped": 0, "status": "ERROR",
                "run_id": run_id, "attempt": attempt,
            })
            file_stats.append(stat)
            print(f"    [ERROR] {exc} — skipping, pipeline continues\n")
            continue

        all_valid.extend(valid)
        all_rejected.extend(rejected)
        file_stats.append(stat)

        print(f"    => {stat['records']} valid  |  {stat['rejected']} rejected  "
              f"|  status={stat['status']}\n")

        registry.record({
            "file_name":         fp.name,
            "sha256":            registry.sha256(fp),
            "mtime":             str(fp.stat().st_mtime),
            "format":            fmt.value,
            "records_total":     stat["records"] + stat["rejected"],
            "records_extracted": stat["records"],
            "records_skipped":   stat["rejected"],
            "status":            "SUCCESS" if stat["records"] > 0 else "PARTIAL",
            "run_id":            run_id,
            "attempt":           attempt,
        })

    if not all_valid:
        print("[WARN] No valid records produced. Check logs.")
        audit.log_summary(0, len(file_stats), time.time() - t0)
        sys.exit(0)

    # ── PHASE 3: Analytics & Reporting ───────────────────────────────────────
    print(f"[PHASE 3] Analytics & Reporting  ({len(all_valid)} records)\n")

    metrics     = aggregate(all_valid)
    psp_summary = aggregate_by_psp(all_valid)
    anomalies   = detect_anomalies(metrics, all_valid)

    if anomalies:
        crit = [a for a in anomalies if a["severity"] == "CRITICAL"]
        high = [a for a in anomalies if a["severity"] == "HIGH"]
        print(f"  Anomalies: {len(anomalies)} total  |  "
              f"CRITICAL={len(crit)}  HIGH={len(high)}")

    if requires_human_approval(anomalies):
        choice = _anomaly_approval(anomalies)
        if choice == "R":
            print("  [USER] Run rejected — outputs will NOT be written.")
            audit.log_summary(len(all_valid), len(file_stats), time.time() - t0)
            sys.exit(0)
        else:
            print("  [USER] Run approved — continuing.\n")
            for stat in file_stats:
                stat["human_flags"].append("ANOMALY_APPROVED_BY_USER")

    # Write outputs
    norm_path    = data_dir    / f"withdrawals_{ts}.csv"
    reject_path  = data_dir    / f"rejected_{ts}.csv"
    metrics_path = results_dir / f"metrics_{ts}.csv"
    report_path  = results_dir / f"report_{ts}.md"

    _write_csv(all_valid,    norm_path)
    _write_csv(all_rejected, reject_path)
    _write_csv(metrics,      metrics_path)

    gen_report(
        all_records  = all_valid,
        metrics      = metrics,
        psp_summary  = psp_summary,
        anomalies    = anomalies,
        file_stats   = file_stats,
        run_id       = run_id,
        elapsed_sec  = time.time() - t0,
        out_path     = report_path,
    )

    # ── PHASE 4: Audit & Registry ─────────────────────────────────────────────
    print("[PHASE 4] Audit & Registry Update\n")

    for stat in file_stats:
        file_records = [r for r in all_valid if r.get("source_file") == stat["file"]]
        file_anom    = [a for a in anomalies
                        if a["psp"] in {r.get("psp","") for r in file_records} or a["psp"] == "ALL"]
        audit.log_file(
            file_name         = stat["file"],
            fmt               = stat["format"],
            fmt_confidence    = stat["conf"],
            records_processed = stat["records"],
            records_skipped   = stat["rejected"],
            records_rejected  = stat["rejected"],
            parsing_method    = stat["parsing_method"],
            field_mappings    = stat["field_mappings"],
            valid_records     = file_records,
            errors            = stat["errors"],
            human_flags       = stat["human_flags"],
            anomalies         = file_anom,
        )

    elapsed = time.time() - t0
    audit.log_summary(len(all_valid), len(file_stats), elapsed)

    # ── summary ───────────────────────────────────────────────────────────────
    print(f"{'='*60}")
    print(f"  PIPELINE COMPLETE  |  {elapsed:.2f}s")
    print(f"{'='*60}")
    print(f"  Normalized  : {norm_path.relative_to(_ROOT)}")
    print(f"  Rejected    : {reject_path.relative_to(_ROOT)}")
    print(f"  Metrics     : {metrics_path.relative_to(_ROOT)}")
    print(f"  Report      : {report_path.relative_to(_ROOT)}")
    print(f"  Audit log   : {audit.path.relative_to(_ROOT)}")
    print(f"  Records     : {len(all_valid)} valid  |  {len(all_rejected)} rejected")
    print(f"  PSPs        : {len(psp_summary)}")
    print(f"  Metric rows : {len(metrics)}")
    print(f"  Anomalies   : {len(anomalies)}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
