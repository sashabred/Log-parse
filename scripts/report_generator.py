"""
report_generator.py — Generates a timestamped Markdown analysis report.

Output: results/report_[TIMESTAMP].md
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


def generate(
    all_records:  list[dict],
    metrics:      list[dict],
    psp_summary:  list[dict],
    anomalies:    list[dict],
    file_stats:   list[dict],
    run_id:       str,
    elapsed_sec:  float,
    out_path:     Path,
) -> None:
    lines: list[str] = []
    _section = lambda title: lines.extend([f"\n## {title}", ""])
    _rule     = lambda: lines.append("---")

    # ── header ────────────────────────────────────────────────────────────────
    lines += [
        "# Withdrawal Log Analysis Report",
        "",
        f"| Field | Value |",
        f"|---|---|",
        f"| **Run ID** | `{run_id}` |",
        f"| **Generated** | {_now()} |",
        f"| **Elapsed** | {elapsed_sec:.2f}s |",
        f"| **Total Records** | {len(all_records)} |",
        f"| **Files Processed** | {len(file_stats)} |",
        "",
    ]
    _rule()

    # ── file inventory ────────────────────────────────────────────────────────
    _section("File Inventory")
    lines += [
        "| File | Format | Confidence | Records | Status |",
        "|---|---|---|---|---|",
    ]
    for fs in file_stats:
        conf_badge = _conf_badge(fs.get("conf", 0))
        lines.append(
            f"| `{fs['file']}` | `{fs['format']}` | {conf_badge} "
            f"| {fs['records']} | {fs['status']} |"
        )
    lines.append("")

    # ── executive summary ─────────────────────────────────────────────────────
    _section("Executive Summary")
    total = len(all_records)
    success_n  = sum(1 for r in all_records if r.get("status") == "SUCCESS")
    failed_n   = sum(1 for r in all_records if r.get("status") == "FAILED")
    declined_n = sum(1 for r in all_records if r.get("status") == "DECLINED")
    lats       = [r["latency_ms"] for r in all_records if r.get("latency_ms")]
    vols       = [r["volume"]     for r in all_records if r.get("volume")]

    lines += [
        f"| Metric | Value |",
        f"|---|---|",
        f"| Total Transactions | **{total}** |",
        f"| Successful | {success_n} ({_pct(success_n, total)}%) |",
        f"| Failed | {failed_n} ({_pct(failed_n, total)}%) |",
        f"| Declined | {declined_n} ({_pct(declined_n, total)}%) |",
        f"| Avg Latency | {round(sum(lats)/len(lats), 1) if lats else 'N/A'} ms |",
        f"| Total Volume | {round(sum(vols), 2) if vols else 'N/A'} |",
        "",
    ]

    # ── anomalies ─────────────────────────────────────────────────────────────
    if anomalies:
        _section("Anomalies & Alerts")
        lines += [
            "| Severity | Detector | PSP | Bank | Country | Metric | Value | Threshold |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for a in anomalies:
            sev_badge = _sev_badge(a["severity"])
            lines.append(
                f"| {sev_badge} | `{a['detector']}` | {a['psp']} | {a['bank']} "
                f"| {a['country']} | `{a['metric']}` | {a['value']} | {a['threshold']} |"
            )
        approval_needed = [a for a in anomalies if a.get("requires_approval")]
        if approval_needed:
            lines += [
                "",
                f"> **{len(approval_needed)} anomaly(ies) require human approval** "
                f"before results are considered final.",
            ]
        lines.append("")
    else:
        _section("Anomalies & Alerts")
        lines += ["> No anomalies detected.", ""]

    # ── PSP summary ───────────────────────────────────────────────────────────
    _section("PSP Performance Summary")
    lines += [
        "| PSP | Ops | Success% | Decline% | Error% | Avg Lat (ms) | P95 Lat (ms) | Volume |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in psp_summary:
        lines.append(
            f"| **{row['psp']}** | {row['total_ops']} "
            f"| {row['success_rate_%']}% | {row['decline_rate_%']}% "
            f"| {row['error_rate_%']}% | {row['avg_latency_ms']} "
            f"| {row['p95_latency_ms']} | {row['transaction_total_volume']} |"
        )
    lines.append("")

    # ── error breakdown ───────────────────────────────────────────────────────
    _section("Error & Decline Breakdown")
    err_counts: dict[str, int] = {}
    for r in all_records:
        e = r.get("error", "")
        if e:
            err_counts[e] = err_counts.get(e, 0) + 1
    if err_counts:
        lines += [
            "| Error Code | Count | Rate |",
            "|---|---|---|",
        ]
        for code, cnt in sorted(err_counts.items(), key=lambda x: -x[1]):
            lines.append(f"| `{code}` | {cnt} | {_pct(cnt, total)}% |")
    else:
        lines.append("> No errors recorded.")
    lines.append("")

    # ── metrics table (top 25) ────────────────────────────────────────────────
    _section(f"Metrics Table (top {min(25, len(metrics))} of {len(metrics)} rows)")
    cols = [
        "psp", "bank", "country", "total_ops",
        "success_rate_%", "decline_rate_%", "error_rate_%",
        "job_completion_rate_%", "avg_latency_ms", "p95_latency_ms",
        "transaction_total_volume",
    ]
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
    for row in metrics[:25]:
        lines.append("| " + " | ".join(str(row.get(c, "")) for c in cols) + " |")
    lines.append("")

    # ── confidence summary ────────────────────────────────────────────────────
    confs = [r.get("confidence", 0) for r in all_records]
    if confs:
        avg_c = sum(confs) / len(confs)
        high  = sum(1 for c in confs if c >= 0.85)
        med   = sum(1 for c in confs if 0.70 <= c < 0.85)
        low   = sum(1 for c in confs if 0.50 <= c < 0.70)
        crit  = sum(1 for c in confs if c < 0.50)
        _section("Confidence Distribution")
        lines += [
            f"| Band | Count | Description |",
            f"|---|---|---|",
            f"| HIGH (>=0.85) | {high} | Auto-processed, no review needed |",
            f"| MEDIUM (0.70-0.84) | {med} | Auto-processed, flagged |",
            f"| LOW (0.50-0.69) | {low} | Requires human review |",
            f"| CRITICAL (<0.50) | {crit} | Rejected, written to rejected CSV |",
            f"",
            f"> **Average confidence: {avg_c:.3f}**",
            "",
        ]

    # ── severity breakdown ────────────────────────────────────────────────────
    _section("Severity Breakdown")
    sev_counts: dict[str, int] = {}
    for r in all_records:
        s = r.get("severity", "INFO")
        sev_counts[s] = sev_counts.get(s, 0) + 1
    lines += [
        "| Severity | Count | Rate |",
        "|---|---|---|",
    ]
    for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"]:
        cnt = sev_counts.get(sev, 0)
        lines.append(f"| {_sev_badge(sev)} | {cnt} | {_pct(cnt, total)}% |")
    lines.append("")

    _rule()
    lines += [
        "",
        f"*Report generated by Withdrawal Log Analysis Pipeline — Run `{run_id}`*",
    ]

    out_path.write_text("\n".join(lines), encoding="utf-8")


# ── helpers ───────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _pct(num: int | float, den: int | float) -> str:
    return f"{num / den * 100:.1f}" if den else "0.0"


def _conf_badge(c: float) -> str:
    if c >= 0.85: return f"`{c:.2f}` HIGH"
    if c >= 0.70: return f"`{c:.2f}` MEDIUM"
    if c >= 0.50: return f"`{c:.2f}` LOW"
    return f"`{c:.2f}` CRITICAL"


def _sev_badge(s: str) -> str:
    return {
        "CRITICAL": "CRITICAL",
        "HIGH":     "HIGH",
        "MEDIUM":   "MEDIUM",
        "LOW":      "LOW",
        "INFO":     "INFO",
    }.get(s, s)
