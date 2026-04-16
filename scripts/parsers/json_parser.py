"""
json_parser.py — Parser for JSON Lines and JSON Array log files.

JSON Lines: each line is a standalone JSON object  { ... }
JSON Array: entire file is a top-level array       [ {...}, {...} ]

One-level nested dicts are flattened; known field name variants are aliased
to canonical names via the ALIAS_TABLE.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

from .base_parser import BaseParser

ALIAS_TABLE: dict[str, str] = {
    "psp_name":          "psp",
    "provider":          "psp",
    "processor":         "psp",
    "bank_name":         "bank",
    "institution":       "bank",
    "issuer":            "bank",
    "country_code":      "country",
    "region":            "country",
    "iso_country":       "country",
    "latency":           "latency_ms",
    "duration_ms":       "latency_ms",
    "response_time":     "latency_ms",
    "elapsed":           "latency_ms",
    "transaction_amount":"amount",
    "value":             "amount",
    "error_code":        "error",
    "failure_reason":    "error",
    "decline_reason":    "error",
    "tx_id":             "id",
    "transaction_id":    "id",
    "ref_id":            "id",
    "correlation_id":    "guid",
    "trace_id":          "guid",
    "event":             "event_type",
    "type":              "event_type",
}


class JsonParser(BaseParser):
    format_name = "json_lines"   # overridden to json_array when needed

    def parse(self) -> Iterator[dict]:
        content = self.file_path.read_text(encoding="utf-8", errors="replace").strip()
        if not content:
            return

        if content.startswith("["):
            self.format_name = "json_array"
            yield from self._parse_array(content)
        else:
            self.format_name = "json_lines"
            yield from self._parse_lines(content)

    # ── private ───────────────────────────────────────────────────────────────

    def _parse_array(self, content: str) -> Iterator[dict]:
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            return
        if not isinstance(data, list):
            return
        chunk: list[dict] = []
        for i, obj in enumerate(data):
            if not isinstance(obj, dict):
                continue
            rec = self._enrich(self._flatten_alias(obj), i + 1)
            if self.is_withdrawal_event(rec):
                chunk.append(rec)
                if len(chunk) >= self.CHUNK_SIZE:
                    yield from chunk
                    chunk = []
        yield from chunk

    def _parse_lines(self, content: str) -> Iterator[dict]:
        chunk: list[dict] = []
        for line_no, line in enumerate(content.splitlines(), start=1):
            line = line.strip()
            if not line or not line.startswith("{"):
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            rec = self._enrich(self._flatten_alias(obj), line_no)
            if self.is_withdrawal_event(rec):
                chunk.append(rec)
                if len(chunk) >= self.CHUNK_SIZE:
                    yield from chunk
                    chunk = []
        yield from chunk

    def _flatten_alias(self, obj: dict) -> dict:
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

    def _enrich(self, rec: dict, line_no: int) -> dict:
        rec.setdefault("source_line",   line_no)
        rec.setdefault("source_format", self.format_name)
        rec.setdefault("source_file",   self.file_path.name)
        rec.setdefault("request",       "POST /withdrawals/process")
        rec.setdefault("event_type",    "request")
        rec.setdefault("guid",          "")
        rec.setdefault("error",         "")
        return rec
