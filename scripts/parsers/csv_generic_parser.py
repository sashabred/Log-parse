"""
csv_generic_parser.py — Parser for CSV files with a header row.

Uses csv.DictReader; maps header names through the ALIAS_TABLE from
json_parser.py.  Any column not in the alias table is kept under its
original (lowercased) name for downstream adaptive handling.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterator

from .base_parser import BaseParser
from .json_parser import ALIAS_TABLE


class CsvGenericParser(BaseParser):
    format_name = "csv_generic"

    def parse(self) -> Iterator[dict]:
        with self.file_path.open(encoding="utf-8", errors="replace", newline="") as fh:
            reader = csv.DictReader(fh)
            chunk: list[dict] = []
            for line_no, raw_row in enumerate(reader, start=2):  # row 1 = header
                rec = self._map_row(raw_row, line_no)
                if rec and self.is_withdrawal_event(rec):
                    chunk.append(rec)
                    if len(chunk) >= self.CHUNK_SIZE:
                        yield from chunk
                        chunk = []
            yield from chunk

    # ── private ───────────────────────────────────────────────────────────────

    def _map_row(self, row: dict, line_no: int) -> dict:
        mapped: dict = {
            "source_line":   line_no,
            "source_format": self.format_name,
            "source_file":   self.file_path.name,
            "request":       "POST /withdrawals/process",
            "event_type":    "request",
            "guid":          "",
            "error":         "",
        }
        for k, v in row.items():
            if k is None:
                continue
            canonical = ALIAS_TABLE.get(k.lower().strip(), k.lower().strip())
            mapped[canonical] = v.strip() if isinstance(v, str) else v

        # numeric coercions
        mapped["latency_ms"] = self.safe_float(mapped.get("latency_ms"))
        mapped["amount"]     = self.safe_float(mapped.get("amount"))
        return mapped
