"""
freetext_parser.py — Parser for numbered free-text withdrawal logs.

Primary pattern:
  N. YYYY/MM/DD HH:MM:SS - Withdrawal processed/failed - PSP: X, Bank: Y,
  Country: Z, Status: S, Amount: A, Latency: Lms[, Reason: R]

Fallback: generic KV scan for any line containing a withdrawal keyword.
Blank lines and section separators are silently skipped.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterator

from .base_parser import BaseParser

_RE_FULL = re.compile(
    r"^\s*(?P<line_no>\d+)\.\s+"
    r"(?P<timestamp>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})"
    r"\s+-\s+Withdrawal\s+(?P<event_word>processed|failed)"
    r"\s+-\s+PSP:\s*(?P<psp>[^,]+)"
    r",\s*Bank:\s*(?P<bank>[^,]+)"
    r",\s*Country:\s*(?P<country>[^,]+)"
    r",\s*Status:\s*(?P<status>[^,]+)"
    r",\s*Amount:\s*(?P<amount>[\d.]+)"
    r",\s*Latency:\s*(?P<latency>[\d.]+)\s*ms"
    r"(?:,\s*Reason:\s*(?P<error>\S+))?",
    re.IGNORECASE,
)

_RE_KV = re.compile(
    r"(?:PSP)[:\s]+([^\s,]+)"
    r"|(?:Bank)[:\s]+([^\s,]+)"
    r"|(?:Country)[:\s]+([^\s,]+)"
    r"|(?:Status)[:\s]+([A-Z]+)"
    r"|(?:Amount)[:\s]+([\d.]+)"
    r"|(?:Latency)[:\s]+([\d.]+)"
    r"|(?:Reason)[:\s]+([^\s,]+)",
    re.IGNORECASE,
)

_WITHDRAWAL_RE = re.compile(r"\bwithdrawal\b", re.IGNORECASE)


class FreetextParser(BaseParser):
    format_name = "freetext_numbered"

    def parse(self) -> Iterator[dict]:
        chunk: list[dict] = []
        with self.file_path.open(encoding="utf-8", errors="replace") as fh:
            for raw in fh:
                line = raw.strip()
                if not line:
                    continue
                rec = self._parse_line(line)
                if rec and self.is_withdrawal_event(rec):
                    chunk.append(rec)
                    if len(chunk) >= self.CHUNK_SIZE:
                        yield from chunk
                        chunk = []
        yield from chunk

    # ── private ───────────────────────────────────────────────────────────────

    def _parse_line(self, line: str) -> dict | None:
        m = _RE_FULL.match(line)
        if m:
            return self._from_full(m)
        if _WITHDRAWAL_RE.search(line):
            return self._from_kv(line)
        return None

    def _from_full(self, m: re.Match) -> dict:
        return {
            "source_line":   int(m.group("line_no")),
            "timestamp":     m.group("timestamp").strip(),
            "id":            f"line-{m.group('line_no')}",
            "request":       "POST /withdrawals/process",
            "guid":          "",
            "event_type":    "request",
            "psp":           m.group("psp").strip(),
            "bank":          m.group("bank").strip(),
            "country":       m.group("country").strip(),
            "status":        m.group("status").strip().upper(),
            "amount":        float(m.group("amount")),
            "latency_ms":    float(m.group("latency")),
            "error":         (m.group("error") or "").strip().upper(),
            "request_body":  "",
            "source_format": self.format_name,
            "source_file":   self.file_path.name,
        }

    def _from_kv(self, line: str) -> dict:
        keys = ["psp", "bank", "country", "status", "amount", "latency_ms", "error"]
        kv: dict[str, str] = {}
        for m in _RE_KV.finditer(line):
            for i, key in enumerate(keys):
                val = m.group(i + 1)
                if val:
                    kv[key] = val.strip()
        if not kv:
            return {}
        return {
            "source_line":   0,
            "timestamp":     "",
            "id":            "",
            "request":       "POST /withdrawals/process",
            "guid":          "",
            "event_type":    "request",
            "psp":           kv.get("psp", ""),
            "bank":          kv.get("bank", ""),
            "country":       kv.get("country", ""),
            "status":        kv.get("status", "").upper(),
            "amount":        self.safe_float(kv.get("amount")),
            "latency_ms":    self.safe_float(kv.get("latency_ms")),
            "error":         kv.get("error", "").upper(),
            "request_body":  "",
            "source_format": self.format_name,
            "source_file":   self.file_path.name,
        }
