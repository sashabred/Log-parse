# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the Pipeline

```bash
# Standard run — only processes new/changed files (registry-based dedup)
python scripts/run_pipeline.py

# Reprocess all files regardless of registry
python scripts/run_pipeline.py --all

# Override default directories
python scripts/run_pipeline.py --logs Logs --data data --results results --trigger scheduled
```

All scripts must be run from the **workspace root** (`c:/Users/snq9172/Desktop/Log parse/`). The orchestrator resolves paths relative to `_ROOT = Path(__file__).resolve().parent.parent`.

## Directory Constraints (Enforced by Design)

| Directory | Access |
|---|---|
| `Logs/` | **Read-only** — source log files, never modified |
| `scripts/` | **Write** — all pipeline code + `processed_registry.txt` |
| `data/normalized/` | **Write** — versioned output CSVs (`withdrawals_[TS].csv`, `rejected_[TS].csv`) |
| `results/` | **Write** — versioned reports (`report_[TS].md`, `metrics_[TS].csv`) |
| `Logs/audit/` | **Write** — JSONL audit trail (`run_[TS].txt`) |

Output files are **never overwritten** — each run appends a UTC timestamp suffix.

## Architecture: Data Flow

```
Logs/ files
    └─► format_detector.py      # Two-stage detection: extension → structural sniffing
         │                       # Returns (LogFormat enum, confidence float 0–1)
         │
    └─► parsers/                 # One parser per known format
    │    ├─ azure_csv_parser.py  # Unheadered CSV with JSON body column (""key"" escaping)
    │    ├─ freetext_parser.py   # Numbered lines: "N. YYYY/MM/DD - Withdrawal ... PSP: X"
    │    ├─ json_parser.py       # JSON Lines or JSON Array; flattens + alias-maps fields
    │    └─ csv_generic_parser.py# Header-based CSV via DictReader + ALIAS_TABLE
    │
    └─► adaptive_parser.py       # Fallback for unknown/low-confidence formats
         │                        # Tier 0: format_registry.json (approved learned mappings)
         │                        # Tier 1–3: JSON array → JSON lines → CSV DictReader
         │                        # Tier 4: line-level regex KV extraction
         │                        # Emits DECISION_REQUEST sentinel if conf < 0.70
         │                        # and no approved registry entry exists
         │
    └─► format_learner.py        # Structural analysis for unknown files
         │                        # Infers field_mapping from column names / JSON keys / regex
         │                        # Persists learned mappings to format_registry.json
         │
    └─► normalizer.py            # Maps any parser output → unified 20-field schema
         │                        # Validates required fields, ISO timestamps, value ranges
         │                        # Assigns confidence score + severity label
         │
    └─► aggregator.py            # Groups by (psp, bank, country) → 21-metric rows
         │                        # Also aggregate_by_psp() for PSP-level summary
         │
    └─► anomaly_detector.py      # Rule-based checks on aggregated metrics
         │
    └─► report_generator.py      # Writes results/report_[TS].md
    └─► audit_logger.py          # Writes Logs/audit/run_[TS].txt (JSONL)
    └─► registry.py              # Tracks processed files by SHA-256 + mtime
```

`run_pipeline.py` is the sole entry point — it imports and orchestrates all of the above.

## Key Design Patterns

**Parser interface** (`scripts/parsers/base_parser.py`): Every parser inherits `BaseParser`, sets `format_name`, and implements `parse()` as a generator yielding raw dicts. `is_withdrawal_event()` uses a three-tier keyword strategy (exact path → key presence → keyword scan).

**Field alias table** (`scripts/parsers/json_parser.py:ALIAS_TABLE`): The canonical mapping from variant field names (e.g. `psp_name`, `provider`, `processor`) to unified schema names. Both `JsonParser` and `CsvGenericParser` import this table — add new aliases here to cover new formats without changing parsers.

**DECISION_REQUEST protocol**: When `AdaptiveParser` is invoked with `format_confidence < 0.70` AND no approved registry entry exists, it yields a single sentinel dict `{"__type__": "DECISION_REQUEST", ...}` that also contains a `mapping_proposal` inferred by `FormatLearner`. The orchestrator in `run_pipeline.py` intercepts this, prints the proposal, and either approves (A — persists + marks `approved: true` in `format_registry.json`), rejects (R), or modifies (M). Once approved, the format is looked up as Tier 0 on all subsequent runs with no human pause.

**Confidence scoring** (`scripts/normalizer.py:_confidence`): `field_coverage × format_confidence × source_quality`. Source quality constants are hardcoded per format (rules-based parsers = 1.0, adaptive = 0.65). Records with `confidence < 0.50` are written to `rejected_[TS].csv`, not to the normalized output.

**Status normalisation**: `DECLINED` is surfaced as a separate logical status (not `FAILED`) when `status == FAILED` and `error == DECLINED`. This distinction flows through to `decline_ops` vs `error_count` in the metrics table.

**Chunked processing**: All parsers yield in chunks of `CHUNK_SIZE = 1000` rows to bound memory. The orchestrator processes full files but individual parsers flush chunks during iteration.

## Adding a New Log Format

1. Create `scripts/parsers/<name>_parser.py` extending `BaseParser`; set `format_name`; implement `parse()` as a generator.
2. Add a detection heuristic to `scripts/format_detector.py` (`LogFormat` enum + regex/structural rule in `detect()`).
3. Register the new `LogFormat → ParserClass` entry in `_FORMAT_PARSERS` in `run_pipeline.py`.
4. If the new format introduces new field names, add aliases to `ALIAS_TABLE` in `scripts/parsers/json_parser.py`.

For unknown/one-off formats the pipeline uses `AdaptiveParser` automatically. To teach it a new format permanently, run `/optimize-adaptive-parser` (see the skill below) which runs `FormatLearner`, shows the inferred mapping, and writes to `format_registry.json` on approval.

To add a new generic extraction strategy to `AdaptiveParser`, implement `_try_<name>(self, lines, content) -> list[dict]` and append it to `_STRATEGIES`.

## Unified Output Schema

All 150 fields below are always present in `data/normalized/withdrawals_*.csv`:

`id` · `guid` · `request` · `request_body` · `timestamp` (ISO-8601) · `response` · `latency_ms` · `volume` · `psp` · `bank` · `country` · `status` (SUCCESS/FAILED/DECLINED/UNKNOWN) · `error` · `event_type` · `source_file` · `source_format` · `validation_flags` · `is_valid` · `confidence` · `severity` (CRITICAL/HIGH/MEDIUM/LOW/INFO)

## Anomaly Detection Thresholds

Defined as module-level constants in `scripts/anomaly_detector.py` — edit there to tune:

| Detector | Default threshold |
|---|---|
| `FAILURE_SPIKE` | `error_rate > 40%` |
| `HIGH_ERROR_RATE` | `error_rate > 25%` |
| `HIGH_DECLINE_RATE` | `decline_rate > 30%` |
| `LATENCY_DEGRADATION` | `p95 > 900 ms` |
| `ZERO_SUCCESS` | `success == 0` with `total_ops >= 5` |
| `CONFIDENCE_DROP` | `avg_confidence < 0.70` |

`CRITICAL` and `HIGH` anomalies set `requires_approval = True`, causing the orchestrator to emit a `DECISION_REQUEST` before writing outputs.

## What Counts as an Incorrect or Unknown Format

There are two distinct failure levels. Both are fully documented in the module
docstrings of `scripts/adaptive_parser.py` and `scripts/format_learner.py`.

### File-level (format detection failure)

| Confidence from `format_detector.detect()` | Meaning | Pipeline action |
|---|---|---|
| `0.00 – 0.29` | No structural heuristic matched at all | `LogFormat.UNKNOWN` → `AdaptiveParser` + `FormatLearner` invoked |
| `0.30 – 0.59` | A heuristic matched < 40% of sampled lines | `LogFormat.UNKNOWN` → `AdaptiveParser` + `FormatLearner` invoked |
| `0.60 – 0.69` | Moderate match, below safe threshold | `DECISION_REQUEST` emitted with `FormatLearner` proposal |
| `0.70 – 1.00` | Format reliably identified | Auto-parsed, no pause |

A file re-enters normal parsing (no `DECISION_REQUEST`) after its fingerprint is
approved in `format_registry.json` via `/optimize-adaptive-parser`.

### Record-level (schema validation failure after parsing)

Defined in `scripts/normalizer.py`. A record is **rejected** (written to
`data/normalized/rejected_[TS].csv`) if any of these conditions hold:

| Validation flag | Condition | Effect |
|---|---|---|
| `MISSING_PSP` | `psp` field empty or null | Record rejected |
| `MISSING_TIMESTAMP` | timestamp field absent | Record rejected |
| `UNPARSEABLE_TIMESTAMP` | timestamp present but matches no known format | Record rejected |
| `DUPLICATE_ID` | same `id` seen twice in one file | Second record rejected |
| `confidence < 0.50` | fewer than 3 of 6 core fields extracted | Record rejected |
| `INVALID_STATUS:<val>` | status not in `SUCCESS/FAILED/DECLINED/UNKNOWN` | Status coerced to `UNKNOWN`, record kept |
| `LATENCY_OUT_OF_RANGE` | `latency_ms` outside `(0, 300 000)` ms | Field nulled, record kept |
| `VOLUME_OUT_OF_RANGE` | `volume` outside `(0, 1 000 000)` | Field nulled, record kept |

If more than 20% of a file's records are rejected, the orchestrator appends
`HIGH_REJECTION_RATE` to `human_flags` in the audit log.

## `/optimize-adaptive-parser` Skill

Invoke with `/optimize-adaptive-parser` to scan `Logs/` for files the pipeline
could not confidently parse, run `FormatLearner` on each, and approve/reject the
inferred field mappings interactively.

Approved mappings are stored in `scripts/format_registry.json` and loaded by
`AdaptiveParser` as **Tier 0** on future runs — so every unknown format is only
unknown once.

The skill's full instructions are in
`.claude/commands/optimize-adaptive-parser.md`.

## Format Registry (`scripts/format_registry.json`)

Keyed by a structural fingerprint (MD5 of sorted column/key names). Each entry:

```json
{
  "<fingerprint>": {
    "format_name":        "learned_csv_a1b2c3",
    "structural_type":    "tabular",
    "extraction_hint":    "csv_headers",
    "field_mapping":      { "proc_name": "psp", "resp_ms": "latency_ms" },
    "learned_confidence": 0.833,
    "approved":           true,
    "times_seen":         4,
    "file_examples":      ["new_psp_export.csv"]
  }
}
```

`approved: false` → still triggers `DECISION_REQUEST`. `approved: true` → Tier 0
auto-parse, no human pause needed. Edit `approved` directly to revoke or
grant access.

## Registry

`scripts/processed_registry.txt` is a JSONL file tracking every processed file by `(file_name, sha256)`. Files with `status == SUCCESS` are skipped on subsequent runs. `status == ERROR` or `PARTIAL` files are retried up to `MAX_RETRIES = 3` (defined in `scripts/registry.py`). Pass `--all` to bypass the registry entirely.
