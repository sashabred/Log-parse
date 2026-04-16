"""
base_parser.py — Abstract base class for all format-specific parsers.

Every parser must:
  - Accept file_path in __init__
  - Implement parse() as a generator yielding raw dicts
  - Set class attribute format_name

Withdrawal detection uses a tiered keyword strategy (see is_withdrawal_event).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator

# Tier 1 — exact path / key signals
_TIER1_PATHS = {"/withdrawals/", "/withdraw", "/payout", "/disbursement"}
_TIER1_KEYS  = {"psp", "psp_name", "provider"}
_TIER1_EVENTS= {"withdrawal_request", "withdrawal_response", "payout_initiated"}

# Tier 2 — keyword scan in any string value
_TIER2_KW = {"withdrawal", "withdraw", "payout", "disbursement", "remittance"}


class BaseParser(ABC):
    format_name: str = "base"
    CHUNK_SIZE: int  = 1000      # rows per processing chunk

    def __init__(self, file_path: str | Path):
        self.file_path = Path(file_path)

    @abstractmethod
    def parse(self) -> Iterator[dict]:
        """Yield one raw dict per log record. Must be a generator."""

    # ── withdrawal detection ──────────────────────────────────────────────────

    def is_withdrawal_event(self, record: dict) -> bool:
        """
        Three-tier detection.

        Tier 1 — exact structural match  (confidence: high)
        Tier 2 — keyword scan            (confidence: medium)
        Tier 3 — caller should use AI    (not done here)
        """
        # Tier 1a: request path
        req = str(record.get("request", "")).lower()
        if any(p in req for p in _TIER1_PATHS):
            return True

        # Tier 1b: PSP key present
        for k in _TIER1_KEYS:
            if record.get(k):
                return True

        # Tier 1c: event type
        evt = str(record.get("event_type", "")).lower()
        if evt in _TIER1_EVENTS:
            return True

        # Tier 2: keyword in any value
        combined = " ".join(str(v).lower() for v in record.values())
        return any(kw in combined for kw in _TIER2_KW)

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def safe_float(value: object) -> float | None:
        try:
            return float(str(value).strip())
        except (ValueError, TypeError):
            return None
