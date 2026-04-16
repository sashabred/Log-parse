"""
azure_csv_parser.py — Parser for Azure-style withdrawal CSV logs.

Column layout (0-indexed, no header row):
  0  timestamp     "MM/DD/YYYY, HH:MM:SS.ffffff"
  1  id            tx-XXXX
  2  request       "POST /withdrawals/process"
  3  guid          (may be empty)
  4  success_bool  True / False
  5  (reserved)
  6  (reserved)
  7  latency_ms    float
  8  perf_bucket   "2sec-5sec" etc.
  9  event_type    "request"
 10+ json_body     {"psp":..., "bank":..., "country":..., "status":...,
                    "amount":..., "latency_ms":..., "error":...}

The JSON body uses CSV double-quote escaping: ""key"" → "key" after parsing.
Files are processed in chunks of CHUNK_SIZE rows.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterator

from .base_parser import BaseParser

_COL = {
    "timestamp":   0,
    "id":          1,
    "request":     2,
    "guid":        3,
    "success":     4,
    "latency_ms":  7,
    "perf_bucket": 8,
    "event_type":  9,
    "json_body":   10,
}


class AzureCsvParser(BaseParser):
    format_name = "azure_withdrawal_csv"

    def parse(self) -> Iterator[dict]:
        with self.file_path.open(encoding="utf-8", errors="replace", newline="") as fh:
            reader = csv.reader(fh)
            chunk: list[dict] = []
            for line_no, row in enumerate(reader, start=1):
                if not row:
                    continue
                rec = self._row_to_dict(row, line_no)
                if rec and self.is_withdrawal_event(rec):
                    chunk.append(rec)
                    if len(chunk) >= self.CHUNK_SIZE:
                        yield from chunk
                        chunk = []
            yield from chunk

    # ── private ───────────────────────────────────────────────────────────────

    def _row_to_dict(self, row: list[str], line_no: int) -> dict | None:
        try:
            body = self._parse_body(row)
            return {
                "source_line":   line_no,
                "timestamp":     self._col(row, "timestamp"),
                "id":            self._col(row, "id"),
                "request":       self._col(row, "request"),
                "guid":          self._col(row, "guid"),
                "success_flag":  self._col(row, "success"),
                "latency_ms":    self.safe_float(self._col(row, "latency_ms")),
                "perf_bucket":   self._col(row, "perf_bucket"),
                "event_type":    self._col(row, "event_type"),
                "psp":           body.get("psp", ""),
                "bank":          body.get("bank", ""),
                "country":       body.get("country", ""),
                "status":        body.get("status", ""),
                "amount":        body.get("amount"),
                "error":         body.get("error", ""),
                "request_body":  json.dumps(body) if body else "",
                "source_format": self.format_name,
                "source_file":   self.file_path.name,
            }
        except Exception:
            return None

    def _col(self, row: list[str], name: str) -> str:
        idx = _COL[name]
        return row[idx].strip() if len(row) > idx else ""

    def _parse_body(self, row: list[str]) -> dict:
        if len(row) <= _COL["json_body"]:
            return {}
        # Rejoin — commas inside the JSON body split it into extra CSV columns
        raw = ",".join(row[_COL["json_body"]:]).strip()
        if not raw:
            return {}
        # Attempt 1: valid JSON as-is (outer-quoted field already unescaped)
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass
        # Attempt 2: unescape CSV double-quote encoding ("" -> ")
        # The JSON body in this format is NOT wrapped in outer quotes, so
        # csv.reader leaves "" intact — we must unescape manually.
        unescaped = raw.replace('""', '"')
        try:
            return json.loads(unescaped)
        except json.JSONDecodeError:
            pass
        # Attempt 3: strip outer quotes then unescape
        if raw.startswith('"') and raw.endswith('"'):
            try:
                return json.loads(raw[1:-1].replace('""', '"'))
            except Exception:
                pass
        return {}
