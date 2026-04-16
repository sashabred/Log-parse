"""
format_learner.py — Structural analysis and format learning for unknown log files.

PURPOSE
-------
When a file arrives that format_detector cannot confidently identify (confidence
< DECISION_THRESHOLD), this module:
  1. Analyses the file's structural fingerprint (column names / JSON keys / regex patterns)
  2. Scores each field against the canonical schema using a heuristic similarity table
  3. Proposes a field_mapping dict (raw_name → canonical_name)
  4. Computes a learned_confidence score
  5. Persists the learned mapping to scripts/format_registry.json

On subsequent runs AdaptiveParser loads the registry and applies the learned mapping
as a Tier 0 strategy before falling back to generic extraction — so every unknown
format is only "unknown" once.

WHAT COUNTS AS AN INCORRECT / UNKNOWN FORMAT
---------------------------------------------
A file is treated as incorrect or unknown at two distinct levels:

  FILE LEVEL (format detection failure)
  ┌────────────────────────────────────────────────────────────────────────┐
  │ Condition                          │ Confidence  │ Pipeline action      │
  │────────────────────────────────────┼─────────────┼──────────────────────│
  │ No structural heuristic matches    │ 0.00–0.29   │ UNKNOWN → FormatLearner│
  │ Partial match (<40% of sample)     │ 0.30–0.59   │ UNKNOWN → FormatLearner│
  │ Moderate match (40–60% of sample)  │ 0.60–0.69   │ DECISION_REQUEST emitted│
  │ Strong match (≥60% of sample)      │ 0.70–1.00   │ Auto-parsed, no pause  │
  └────────────────────────────────────────────────────────────────────────┘

  RECORD LEVEL (schema validation failure after parsing)
  ┌────────────────────────────────────────────────────────────────────────┐
  │ Condition                          │ Flag                  │ Outcome   │
  │────────────────────────────────────┼───────────────────────┼───────────│
  │ psp is empty/null                  │ MISSING_PSP           │ rejected  │
  │ timestamp missing or unparseable   │ MISSING_TIMESTAMP /   │ rejected  │
  │                                    │ UNPARSEABLE_TIMESTAMP │           │
  │ status not in SUCCESS/FAILED/      │ INVALID_STATUS:<val>  │ status→   │
  │   DECLINED/UNKNOWN                 │                       │ UNKNOWN   │
  │ latency_ms outside (0, 300 000) ms │ LATENCY_OUT_OF_RANGE  │ null out  │
  │ volume outside (0, 1 000 000)      │ VOLUME_OUT_OF_RANGE   │ null out  │
  │ duplicate id within same file      │ DUPLICATE_ID          │ rejected  │
  │ field_coverage < 0.50              │ (confidence < 0.50)   │ rejected  │
  └────────────────────────────────────────────────────────────────────────┘

FORMAT REGISTRY
---------------
Persisted to scripts/format_registry.json.  Structure:

{
  "<fingerprint_hash>": {
    "format_name":       str,       # e.g. "learned_csv_v1"
    "structural_type":   str,       # "tabular" | "document" | "freetext"
    "detected_at":       ISO-8601,
    "last_seen":         ISO-8601,
    "times_seen":        int,
    "file_examples":     [str],     # filenames that matched this fingerprint
    "field_mapping":     {str: str},# raw column → canonical name
    "extraction_hint":   str,       # "csv_headers" | "json_keys" | "regex_kv"
    "learned_confidence":float,     # fraction of core fields mapped
    "approved":          bool       # True once a human has approved this mapping
  }
}
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ── canonical schema ──────────────────────────────────────────────────────────

CORE_FIELDS = ("psp", "bank", "country", "status", "amount", "latency_ms")

# Similarity table: raw name patterns → canonical field
# Ordered from most to least specific
_SIMILARITY_RULES: list[tuple[re.Pattern, str]] = [
    (re.compile(r'\bpsp(?:_name|_id|_code)?\b',     re.I), "psp"),
    (re.compile(r'\b(?:provider|processor|acquirer)\b', re.I), "psp"),
    (re.compile(r'\bbank(?:_name|_code|_id)?\b',    re.I), "bank"),
    (re.compile(r'\b(?:issuer|institution|fin_inst)\b', re.I), "bank"),
    (re.compile(r'\bcountry(?:_code|_iso)?\b',      re.I), "country"),
    (re.compile(r'\b(?:region|iso_country|nation)\b', re.I), "country"),
    (re.compile(r'\bstatus(?:_code|_flag)?\b',      re.I), "status"),
    (re.compile(r'\b(?:result|outcome|state)\b',    re.I), "status"),
    (re.compile(r'\bamount(?:_usd|_eur|_gbp)?\b',   re.I), "amount"),
    (re.compile(r'\b(?:transaction_amount|value|sum|total_amount)\b', re.I), "amount"),
    (re.compile(r'\b(?:volume|amt)\b',              re.I), "amount"),
    (re.compile(r'\b(?:latency(?:_ms)?|duration(?:_ms)?|elapsed|response_time|resp_time)\b', re.I), "latency_ms"),
    (re.compile(r'\b(?:error(?:_code)?|failure_reason|decline_reason|reason)\b', re.I), "error"),
    (re.compile(r'\b(?:tx_id|transaction_id|ref(?:erence)?_id|txn_id)\b', re.I), "id"),
    (re.compile(r'\b(?:guid|correlation_id|trace_id|request_id)\b', re.I), "guid"),
    (re.compile(r'\b(?:timestamp|ts|time|datetime|created_at|@timestamp)\b', re.I), "timestamp"),
    (re.compile(r'\b(?:event_type|event|type|action)\b', re.I), "event_type"),
    (re.compile(r'\b(?:request|req|endpoint|path|url)\b', re.I), "request"),
]

_REGISTRY_NAME = "format_registry.json"


# ── data structures ───────────────────────────────────────────────────────────

@dataclass
class LearnedFormat:
    fingerprint:        str
    format_name:        str
    structural_type:    str          # tabular | document | freetext
    field_mapping:      dict[str, str]
    extraction_hint:    str          # csv_headers | json_keys | regex_kv
    learned_confidence: float
    raw_columns:        list[str]    # original column/key names before mapping
    sample_values:      dict[str, list]  # raw_col → [up to 3 sample values]
    file_example:       str
    approved:           bool = False

    def summary(self) -> str:
        mapped = [f"  {k!r:30s} -> {v!r}" for k, v in self.field_mapping.items()]
        unmapped = [c for c in self.raw_columns if c not in self.field_mapping]
        lines = [
            f"Format     : {self.format_name}",
            f"Type       : {self.structural_type}",
            f"Hint       : {self.extraction_hint}",
            f"Confidence : {self.learned_confidence:.3f}",
            f"Fingerprint: {self.fingerprint[:16]}...",
            f"Mapped fields ({len(self.field_mapping)}):",
            *mapped,
        ]
        if unmapped:
            lines.append(f"Unmapped   : {unmapped}")
        return "\n".join(lines)


# ── public API ────────────────────────────────────────────────────────────────

class FormatLearner:
    """
    Analyses an unknown file and produces a LearnedFormat.

    Usage
    -----
    learner = FormatLearner(scripts_dir)
    result  = learner.learn(file_path)
    if result:
        print(result.summary())
        learner.persist(result)   # writes to format_registry.json
    """

    def __init__(self, scripts_dir: Path):
        self._scripts_dir = scripts_dir
        self._registry_path = scripts_dir / _REGISTRY_NAME
        self._registry: dict = self._load_registry()

    def learn(self, file_path: Path) -> LearnedFormat | None:
        """
        Analyse *file_path* and return a LearnedFormat, or None if the file
        is empty or completely unanalysable.
        """
        try:
            content = file_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return None

        lines = [ln for ln in content.splitlines() if ln.strip()]
        if not lines:
            return None

        # Try structural types in order of richness
        for analyser in (_analyse_json_array, _analyse_json_lines, _analyse_csv, _analyse_freetext):
            result = analyser(file_path, lines, content)
            if result is not None:
                return result

        return None

    def lookup(self, file_path: Path) -> LearnedFormat | None:
        """
        Return a previously-learned format for *file_path* if one exists in
        the registry and has been approved, else None.
        """
        fp = _fingerprint_file(file_path)
        entry = self._registry.get(fp)
        if entry and entry.get("approved"):
            return _entry_to_learned(fp, entry, file_path.name)
        return None

    def lookup_by_fingerprint(self, fingerprint: str) -> dict | None:
        return self._registry.get(fingerprint)

    def persist(self, learned: LearnedFormat) -> None:
        """Write *learned* to the registry. Merges with any existing entry."""
        now = _now()
        existing = self._registry.get(learned.fingerprint, {})

        entry = {
            "format_name":       learned.format_name,
            "structural_type":   learned.structural_type,
            "detected_at":       existing.get("detected_at", now),
            "last_seen":         now,
            "times_seen":        existing.get("times_seen", 0) + 1,
            "file_examples":     _dedup(existing.get("file_examples", []) + [learned.file_example]),
            "field_mapping":     learned.field_mapping,
            "extraction_hint":   learned.extraction_hint,
            "learned_confidence": learned.learned_confidence,
            "raw_columns":       learned.raw_columns,
            "sample_values":     learned.sample_values,
            "approved":          existing.get("approved", False),
        }
        self._registry[learned.fingerprint] = entry
        self._save_registry()

    def approve(self, fingerprint: str) -> bool:
        """Mark a registry entry as approved for auto-use. Returns True if found."""
        if fingerprint not in self._registry:
            return False
        self._registry[fingerprint]["approved"] = True
        self._save_registry()
        return True

    def list_pending(self) -> list[dict]:
        """Return all registry entries not yet approved."""
        return [
            {"fingerprint": fp, **entry}
            for fp, entry in self._registry.items()
            if not entry.get("approved")
        ]

    def list_all(self) -> list[dict]:
        return [
            {"fingerprint": fp, **entry}
            for fp, entry in self._registry.items()
        ]

    # ── persistence ───────────────────────────────────────────────────────────

    def _load_registry(self) -> dict:
        if not self._registry_path.exists():
            return {}
        try:
            return json.loads(self._registry_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_registry(self) -> None:
        self._registry_path.write_text(
            json.dumps(self._registry, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )


# ── format analysers ──────────────────────────────────────────────────────────

def _analyse_json_array(fp: Path, lines: list[str], content: str) -> LearnedFormat | None:
    try:
        data = json.loads(content)
    except Exception:
        return None
    if not (isinstance(data, list) and data and isinstance(data[0], dict)):
        return None

    keys = _union_keys(data[:20])
    mapping = _map_fields(keys)
    samples = _sample_values_from_dicts(data[:5], keys)
    fingerprint = _fingerprint_keys(keys)

    return LearnedFormat(
        fingerprint        = fingerprint,
        format_name        = f"learned_json_array_{fingerprint[:6]}",
        structural_type    = "document",
        field_mapping      = mapping,
        extraction_hint    = "json_keys",
        learned_confidence = _confidence(mapping),
        raw_columns        = sorted(keys),
        sample_values      = samples,
        file_example       = fp.name,
    )


def _analyse_json_lines(fp: Path, lines: list[str], _content: str) -> LearnedFormat | None:
    parsed = []
    for ln in lines[:50]:
        if ln.strip().startswith("{"):
            try:
                parsed.append(json.loads(ln))
            except Exception:
                pass
    if not parsed or len(parsed) / max(len(lines), 1) < 0.40:
        return None

    keys = _union_keys(parsed[:20])
    mapping = _map_fields(keys)
    samples = _sample_values_from_dicts(parsed[:5], keys)
    fingerprint = _fingerprint_keys(keys)

    return LearnedFormat(
        fingerprint        = fingerprint,
        format_name        = f"learned_json_lines_{fingerprint[:6]}",
        structural_type    = "document",
        field_mapping      = mapping,
        extraction_hint    = "json_keys",
        learned_confidence = _confidence(mapping),
        raw_columns        = sorted(keys),
        sample_values      = samples,
        file_example       = fp.name,
    )


def _analyse_csv(fp: Path, lines: list[str], _content: str) -> LearnedFormat | None:
    text = "\n".join(lines[:200])
    try:
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
    except Exception:
        return None

    if not rows or not rows[0] or len(rows[0]) < 2:
        return None

    keys = [k for k in rows[0].keys() if k]
    if not keys:
        return None

    mapping = _map_fields(keys)
    samples = _sample_values_from_dicts(rows[:5], keys)
    fingerprint = _fingerprint_keys(keys)

    return LearnedFormat(
        fingerprint        = fingerprint,
        format_name        = f"learned_csv_{fingerprint[:6]}",
        structural_type    = "tabular",
        field_mapping      = mapping,
        extraction_hint    = "csv_headers",
        learned_confidence = _confidence(mapping),
        raw_columns        = keys,
        sample_values      = samples,
        file_example       = fp.name,
    )


def _analyse_freetext(fp: Path, lines: list[str], _content: str) -> LearnedFormat | None:
    """
    Scan lines for recurring KV patterns.
    Build a synthetic 'schema' from which field names were reliably extractable.
    """
    field_hits: dict[str, int] = {f: 0 for f in ("psp", "bank", "country", "status", "amount", "latency_ms", "error", "timestamp")}
    sample_vals: dict[str, list] = {f: [] for f in field_hits}

    kv_patterns = {
        "psp":        re.compile(r'\bpsp[:\s=]+([A-Za-z0-9_\-]+)', re.I),
        "bank":       re.compile(r'\bbank[:\s=]+([A-Za-z0-9_\-]+)', re.I),
        "country":    re.compile(r'\bcountry[:\s=]+([A-Za-z]{2,3})', re.I),
        "status":     re.compile(r'\bstatus[:\s=]+"?([A-Z_]+)"?', re.I),
        "amount":     re.compile(r'\bamount[:\s=]+([\d.]+)', re.I),
        "latency_ms": re.compile(r'\b(?:latency|duration|elapsed)[:\s=]+([\d.]+)', re.I),
        "error":      re.compile(r'\b(?:error|reason)[:\s=]+"?([A-Z_]+)"?', re.I),
        "timestamp":  re.compile(r'(\d{4}[/\-]\d{2}[/\-]\d{2}[\sT]\d{2}:\d{2}:\d{2})'),
    }

    for line in lines[:200]:
        for field_name, pat in kv_patterns.items():
            m = pat.search(line)
            if m:
                field_hits[field_name] += 1
                val = m.group(1).strip()
                if len(sample_vals[field_name]) < 3:
                    sample_vals[field_name].append(val)

    total_lines = len(lines[:200])
    if total_lines == 0:
        return None

    # Only include fields seen in ≥10% of lines
    detected = {f: f"regex:{f}" for f, cnt in field_hits.items() if cnt / total_lines >= 0.10}
    if len(detected) < 2:
        return None

    # Fingerprint: frozenset of detected regex field names
    fingerprint = hashlib.md5("|".join(sorted(detected.keys())).encode()).hexdigest()
    samples_clean = {f: vs for f, vs in sample_vals.items() if vs}

    return LearnedFormat(
        fingerprint        = fingerprint,
        format_name        = f"learned_freetext_{fingerprint[:6]}",
        structural_type    = "freetext",
        field_mapping      = detected,
        extraction_hint    = "regex_kv",
        learned_confidence = _confidence(detected),
        raw_columns        = sorted(detected.keys()),
        sample_values      = samples_clean,
        file_example       = fp.name,
    )


# ── field mapping ─────────────────────────────────────────────────────────────

def _map_fields(columns: list[str]) -> dict[str, str]:
    """
    Map raw column / key names to canonical field names.
    Each raw name is matched against _SIMILARITY_RULES in order;
    first match wins.  A canonical name is claimed by at most one raw name.
    """
    mapping: dict[str, str] = {}
    claimed: set[str] = set()

    for col in columns:
        col_norm = col.strip().lower()
        for pattern, canonical in _SIMILARITY_RULES:
            if canonical in claimed:
                continue
            if pattern.search(col_norm):
                mapping[col] = canonical
                claimed.add(canonical)
                break

    return mapping


def _confidence(mapping: dict[str, str]) -> float:
    core_mapped = sum(1 for v in mapping.values() if v in CORE_FIELDS)
    return round(core_mapped / len(CORE_FIELDS), 3)


# ── fingerprinting ────────────────────────────────────────────────────────────

def _fingerprint_keys(keys: list[str]) -> str:
    """Stable hash of a sorted, lowercased set of column/key names."""
    normalised = sorted(k.strip().lower() for k in keys)
    return hashlib.md5("|".join(normalised).encode()).hexdigest()


def _fingerprint_file(file_path: Path) -> str:
    """
    Compute a structural fingerprint from a file without reading all content.
    Tries JSON keys, then CSV headers, then first-line regex tokens.
    """
    try:
        content = file_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""

    lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
    if not lines:
        return ""

    # JSON array
    try:
        data = json.loads(content)
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return _fingerprint_keys(list(data[0].keys()))
    except Exception:
        pass

    # JSON lines
    for ln in lines[:10]:
        if ln.startswith("{"):
            try:
                return _fingerprint_keys(list(json.loads(ln).keys()))
            except Exception:
                pass

    # CSV headers
    try:
        cols = next(csv.reader(io.StringIO(lines[0])))
        if len(cols) >= 2:
            return _fingerprint_keys(cols)
    except Exception:
        pass

    # Freetext: hash of first 100 chars (rough)
    return hashlib.md5(lines[0][:100].lower().encode()).hexdigest()


# ── utilities ─────────────────────────────────────────────────────────────────

def _union_keys(records: list[dict]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for rec in records:
        for k in rec.keys():
            if k not in seen:
                seen.add(k)
                result.append(k)
    return result


def _sample_values_from_dicts(records: list[dict], keys: list[str]) -> dict[str, list]:
    samples: dict[str, list] = {}
    for key in keys:
        vals = [str(r[key]) for r in records if key in r and r[key] not in (None, "", "null")]
        samples[key] = vals[:3]
    return samples


def _entry_to_learned(fingerprint: str, entry: dict, file_name: str) -> LearnedFormat:
    return LearnedFormat(
        fingerprint        = fingerprint,
        format_name        = entry["format_name"],
        structural_type    = entry["structural_type"],
        field_mapping      = entry["field_mapping"],
        extraction_hint    = entry["extraction_hint"],
        learned_confidence = entry["learned_confidence"],
        raw_columns        = entry.get("raw_columns", []),
        sample_values      = entry.get("sample_values", {}),
        file_example       = file_name,
        approved           = entry.get("approved", False),
    )


def _dedup(lst: list) -> list:
    seen: set = set()
    return [x for x in lst if not (x in seen or seen.add(x))]  # type: ignore[func-returns-value]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
