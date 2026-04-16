"""
adaptive_parser.py — AI-fallback parser for unknown or low-confidence formats.

STRATEGY CASCADE (tried in order, stops at first that yields records)
----------------------------------------------------------------------
Tier 0 — Registry lookup      Load a previously-learned + approved field mapping
                               from format_registry.json for this file's fingerprint.
                               Bypasses all detection if confidence >= 0.70.

Tier 1 — JSON array            Full file is a valid JSON list of dicts.
Tier 2 — JSON lines            >= 50% of lines are standalone JSON objects.
Tier 3 — CSV with headers      csv.DictReader succeeds with >= 3 columns.
Tier 4 — Regex KV fallback     Line-by-line scan using generic field regexes.
                               Only lines matching a withdrawal keyword are processed.

DECISION_REQUEST PROTOCOL
--------------------------
If format_confidence < DECISION_THRESHOLD (0.70) AND no approved registry
entry exists → yield a single sentinel dict with "__type__": "DECISION_REQUEST".
The orchestrator (run_pipeline.py) intercepts this, prints an approval prompt,
and either:
  A  — forces re-parse with elevated confidence
  R  — skips the file, marks registry as SKIPPED
  M  — caller supplies corrected mapping (future extension)

After user approves, FormatLearner.persist() + approve() are called so the
next run uses Tier 0 and never needs human review again.

WHAT MAKES A FORMAT "INCORRECT / UNKNOWN" — quick reference
-------------------------------------------------------------
File level:
  conf 0.00–0.29  No heuristic matched at all           → UNKNOWN, FormatLearner invoked
  conf 0.30–0.59  Partial heuristic match               → UNKNOWN, FormatLearner invoked
  conf 0.60–0.69  Moderate match, below safe threshold  → DECISION_REQUEST emitted
  conf >= 0.70    Format reliably identified             → normal parse, no pause

Record level (see normalizer.py for full list):
  MISSING_PSP / MISSING_TIMESTAMP                       → record rejected
  UNPARSEABLE_TIMESTAMP                                 → record rejected
  INVALID_STATUS:<val>                                  → status coerced to UNKNOWN
  LATENCY_OUT_OF_RANGE / VOLUME_OUT_OF_RANGE            → field nulled out
  DUPLICATE_ID                                          → record rejected
  confidence < 0.50 (< 3 of 6 core fields extracted)   → record rejected
"""

from __future__ import annotations

import csv
import io
import json
import re
from pathlib import Path
from typing import Iterator

from scripts.parsers.base_parser import BaseParser

DECISION_THRESHOLD = 0.70

# ── generic extraction regexes ────────────────────────────────────────────────

_FIELD_RE: dict[str, re.Pattern] = {
    "psp":        re.compile(r'\bpsp\b["\s:=]+([A-Za-z0-9_\-]+)', re.I),
    "bank":       re.compile(r'\bbank\b["\s:=]+([A-Za-z0-9_\-]+)', re.I),
    "country":    re.compile(r'\bcountry\b["\s:=]+([A-Za-z]{2,3})', re.I),
    "status":     re.compile(r'\bstatus\b["\s:=]+"?([A-Z_]+)"?', re.I),
    "amount":     re.compile(r'\bamount\b["\s:=]+([\d.]+)', re.I),
    "latency_ms": re.compile(
        r'\b(?:latency(?:_ms)?|duration(?:_ms)?|elapsed|response_time)\b["\s:=]+([\d.]+)',
        re.I,
    ),
    "error":      re.compile(
        r'\b(?:error|reason|failure_reason|decline_reason)\b["\s:=]+"?([A-Z_]+)"?', re.I
    ),
    "timestamp":  re.compile(r'(\d{4}[/\-]\d{2}[/\-]\d{2}[\sT]\d{2}:\d{2}:\d{2})'),
    "id":         re.compile(r'\b(?:id|tx_id|transaction_id)\b["\s:=]+"?([^\s",]+)"?', re.I),
}

_CORE_FIELDS   = {"psp", "bank", "country", "status", "amount", "latency_ms"}
_WITHDRAWAL_KW = re.compile(
    r'\b(?:withdrawal|withdraw|/withdrawals/|payout|disbursement|remittance)\b', re.I
)

# Lazy import guard for FormatLearner (avoids circular imports at module load)
_LEARNER_AVAILABLE = True
try:
    from scripts.format_learner import FormatLearner, _fingerprint_file
except ImportError:
    _LEARNER_AVAILABLE = False


class AdaptiveParser(BaseParser):
    format_name = "adaptive_unknown"

    def __init__(
        self,
        file_path: str | Path,
        format_confidence: float = 0.30,
        scripts_dir: Path | None = None,
    ):
        super().__init__(file_path)
        self._fmt_conf   = format_confidence
        self._scripts_dir = scripts_dir or Path(__file__).resolve().parent

    # ── public ────────────────────────────────────────────────────────────────

    def parse(self) -> Iterator[dict]:
        """
        Run the Tier 0–4 cascade.
        Yields withdrawal-related records or a single DECISION_REQUEST sentinel.
        """
        content = self.file_path.read_text(encoding="utf-8", errors="replace")
        lines   = content.splitlines()

        # ── Tier 0: registry lookup ───────────────────────────────────────────
        registry_result = self._try_registry(lines, content)
        if registry_result is not None:
            yield from registry_result
            return

        # ── Confidence gate (before Tier 1–4) ────────────────────────────────
        if self._fmt_conf < DECISION_THRESHOLD:
            yield self._decision_request()
            return

        # ── Tier 1–3: structured strategies ──────────────────────────────────
        for strategy in (self._try_json_array, self._try_json_lines, self._try_csv):
            records = strategy(lines, content)
            if records:
                for rec in records:
                    rec.setdefault("source_file",   self.file_path.name)
                    rec.setdefault("source_format", self.format_name)
                    rec["field_coverage"] = self._coverage(rec)
                    if self.is_withdrawal_event(rec):
                        yield rec
                return

        # ── Tier 4: regex KV fallback ─────────────────────────────────────────
        yield from self._regex_fallback(lines)

    # ── Tier 0: registry ─────────────────────────────────────────────────────

    def _try_registry(self, lines: list[str], content: str) -> list[dict] | None:
        """
        If an approved learned format exists for this file's fingerprint,
        apply its field_mapping and return records.
        Returns None if no registry entry found or learner unavailable.
        """
        if not _LEARNER_AVAILABLE:
            return None

        learner = FormatLearner(self._scripts_dir)
        learned = learner.lookup(self.file_path)
        if learned is None:
            return None

        self.format_name = learned.format_name
        hint = learned.extraction_hint
        mapping = learned.field_mapping

        records: list[dict] = []

        if hint == "json_keys":
            records = self._apply_mapping_json(content, mapping)
        elif hint == "csv_headers":
            records = self._apply_mapping_csv(lines, mapping)
        elif hint == "regex_kv":
            records = list(self._regex_fallback_with_mapping(lines, mapping))

        if not records:
            return None

        # Update registry entry: increment times_seen
        learner._registry[_fingerprint_file(self.file_path)]["times_seen"] = \
            learner._registry[_fingerprint_file(self.file_path)].get("times_seen", 0) + 1
        learner._save_registry()

        return [r for r in records if self.is_withdrawal_event(r)]

    def _apply_mapping_json(self, content: str, mapping: dict[str, str]) -> list[dict]:
        records = []
        # Try array first
        try:
            data = json.loads(content)
            if isinstance(data, list):
                for obj in data:
                    if isinstance(obj, dict):
                        records.append(self._remap(obj, mapping))
                return records
        except Exception:
            pass
        # Try lines
        for ln in content.splitlines():
            ln = ln.strip()
            if ln.startswith("{"):
                try:
                    records.append(self._remap(json.loads(ln), mapping))
                except Exception:
                    pass
        return records

    def _apply_mapping_csv(self, lines: list[str], mapping: dict[str, str]) -> list[dict]:
        try:
            reader = csv.DictReader(io.StringIO("\n".join(lines)))
            return [self._remap(dict(row), mapping) for row in reader]
        except Exception:
            return []

    def _regex_fallback_with_mapping(
        self, lines: list[str], mapping: dict[str, str]
    ) -> Iterator[dict]:
        """Regex fallback that renames extracted fields per the learned mapping."""
        for i, line in enumerate(lines, start=1):
            if not _WITHDRAWAL_KW.search(line):
                continue
            rec: dict = {"source_line": i, "raw_line": line.strip()}
            for field_name, pat in _FIELD_RE.items():
                m = pat.search(line)
                if m:
                    canonical = mapping.get(field_name, field_name)
                    val: object = m.group(1).strip()
                    if field_name in {"amount", "latency_ms"}:
                        val = self.safe_float(val) or val
                    rec[canonical] = val
            rec["source_file"]    = self.file_path.name
            rec["source_format"]  = self.format_name
            rec["field_coverage"] = self._coverage(rec)
            if rec["field_coverage"] > 0:
                yield rec

    @staticmethod
    def _remap(obj: dict, mapping: dict[str, str]) -> dict:
        """Apply a field_mapping dict to a raw record dict."""
        result: dict = {}
        for k, v in obj.items():
            canonical = mapping.get(k, k)
            result[canonical] = v
        return result

    # ── Tier 1: JSON array ────────────────────────────────────────────────────

    def _try_json_array(self, lines: list[str], content: str) -> list[dict]:
        try:
            data = json.loads(content)
            if isinstance(data, list) and data and isinstance(data[0], dict):
                return [self._flatten(r) for r in data]
        except Exception:
            pass
        return []

    # ── Tier 2: JSON lines ────────────────────────────────────────────────────

    def _try_json_lines(self, lines: list[str], _: str) -> list[dict]:
        records = []
        for ln in lines:
            ln = ln.strip()
            if ln.startswith("{"):
                try:
                    records.append(self._flatten(json.loads(ln)))
                except Exception:
                    pass
        non_empty = [l for l in lines if l.strip()]
        if non_empty and len(records) / len(non_empty) >= 0.50:
            return records
        return []

    # ── Tier 3: CSV ───────────────────────────────────────────────────────────

    def _try_csv(self, lines: list[str], _: str) -> list[dict]:
        try:
            reader = csv.DictReader(io.StringIO("\n".join(lines)))
            rows = list(reader)
            if rows and len(rows[0]) >= 3:
                return [self._flatten(r) for r in rows]
        except Exception:
            pass
        return []

    # ── Tier 4: regex KV fallback ─────────────────────────────────────────────

    def _regex_fallback(self, lines: list[str]) -> Iterator[dict]:
        chunk: list[dict] = []
        for i, line in enumerate(lines, start=1):
            if not _WITHDRAWAL_KW.search(line):
                continue
            rec = {"source_line": i, "raw_line": line.strip()}
            for field, pat in _FIELD_RE.items():
                m = pat.search(line)
                if m:
                    val: object = m.group(1).strip()
                    if field in {"amount", "latency_ms"}:
                        val = self.safe_float(val) or val
                    rec[field] = val
            rec["source_file"]    = self.file_path.name
            rec["source_format"]  = self.format_name
            rec["field_coverage"] = self._coverage(rec)
            if rec["field_coverage"] > 0:
                chunk.append(rec)
                if len(chunk) >= self.CHUNK_SIZE:
                    yield from chunk
                    chunk = []
        yield from chunk

    # ── decision request ──────────────────────────────────────────────────────

    def _decision_request(self) -> dict:
        """
        Emit a DECISION_REQUEST sentinel.
        If FormatLearner is available, include the inferred mapping proposal.
        """
        try:
            sample = self.file_path.read_text(encoding="utf-8", errors="replace")
            preview = "\n".join(sample.splitlines()[:10])
        except Exception:
            preview = "(could not read file)"

        proposal: dict = {}
        proposal_summary = ""
        if _LEARNER_AVAILABLE:
            try:
                learner = FormatLearner(self._scripts_dir)
                learned = learner.learn(self.file_path)
                if learned:
                    proposal = {
                        "fingerprint":        learned.fingerprint,
                        "field_mapping":      learned.field_mapping,
                        "learned_confidence": learned.learned_confidence,
                        "extraction_hint":    learned.extraction_hint,
                        "structural_type":    learned.structural_type,
                    }
                    proposal_summary = learned.summary()
                    # Persist (unapproved) so the orchestrator can approve later
                    learner.persist(learned)
            except Exception:
                pass

        return {
            "__type__":            "DECISION_REQUEST",
            "file":                self.file_path.name,
            "format_confidence":   self._fmt_conf,
            "reason":              (
                f"Format confidence {self._fmt_conf:.2f} < threshold {DECISION_THRESHOLD}. "
                f"No approved registry entry found."
            ),
            "sample_lines":        preview,
            "mapping_proposal":    proposal,
            "mapping_summary":     proposal_summary,
            "options": {
                "A": "APPROVE — accept inferred mapping, persist to registry, parse now",
                "R": "REJECT  — skip file, mark in registry as SKIPPED",
                "M": "MODIFY  — provide corrected mapping then continue",
            },
        }

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _flatten(obj: dict) -> dict:
        from scripts.parsers.json_parser import ALIAS_TABLE
        flat: dict = {}
        for k, v in obj.items():
            k_norm = ALIAS_TABLE.get(k.lower().strip(), k.lower().strip())
            if isinstance(v, dict):
                for ik, iv in v.items():
                    ik_norm = ALIAS_TABLE.get(ik.lower().strip(), ik.lower().strip())
                    flat[ik_norm] = iv
            else:
                flat[k_norm] = v
        return flat

    @staticmethod
    def _coverage(rec: dict) -> float:
        found = sum(
            1 for f in _CORE_FIELDS
            if rec.get(f) not in (None, "", "N/A", "null", "UNKNOWN")
        )
        return round(found / len(_CORE_FIELDS), 3)
