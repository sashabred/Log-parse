"""
registry.py — Processed-file registry for deduplication and incremental runs.

Registry file: scripts/processed_registry.txt  (JSONL — one entry per file)

Entry schema
------------
{
  "file_name": str,
  "sha256": str,
  "mtime": str,           # ISO-8601
  "processed_at": str,    # ISO-8601
  "format": str,
  "records_total": int,
  "records_extracted": int,
  "records_skipped": int,
  "status": "SUCCESS" | "PARTIAL" | "ERROR",
  "run_id": str,
  "attempt": int
}

A file is considered already-processed when (file_name, sha256) matches an
entry with status == "SUCCESS".  Entries with status ERROR or PARTIAL are
eligible for retry up to MAX_RETRIES attempts.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

MAX_RETRIES = 3
_REGISTRY_NAME = "processed_registry.txt"


class Registry:
    def __init__(self, scripts_dir: Path):
        self._path = scripts_dir / _REGISTRY_NAME
        self._entries: list[dict] = []
        self._load()

    # ── public API ────────────────────────────────────────────────────────────

    def needs_processing(self, file_path: Path) -> tuple[bool, int]:
        """
        Return (should_process, attempt_number).
        - Already succeeded  → (False, 0)
        - Never seen         → (True, 1)
        - ERROR / PARTIAL    → (True, attempt+1) if attempt < MAX_RETRIES
        """
        sha = _sha256(file_path)
        matches = [e for e in self._entries if e["file_name"] == file_path.name]

        if not matches:
            return True, 1

        # most recent entry for this file
        latest = sorted(matches, key=lambda e: e.get("processed_at", ""))[-1]

        if latest["sha256"] != sha:
            # file changed — treat as new
            return True, 1

        if latest["status"] == "SUCCESS":
            return False, 0

        attempt = latest.get("attempt", 1)
        if attempt >= MAX_RETRIES:
            return False, 0  # exhausted retries

        return True, attempt + 1

    def record(self, entry: dict) -> None:
        """Append a processed-file entry and persist to disk."""
        entry.setdefault("processed_at", _now())
        self._entries.append(entry)
        self._save()

    def sha256(self, file_path: Path) -> str:
        return _sha256(file_path)

    def all_entries(self) -> list[dict]:
        return list(self._entries)

    # ── persistence ───────────────────────────────────────────────────────────

    def _load(self) -> None:
        if not self._path.exists():
            self._entries = []
            return
        entries = []
        for line in self._path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        self._entries = entries

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("w", encoding="utf-8") as fh:
            for entry in self._entries:
                fh.write(json.dumps(entry) + "\n")


# ── helpers ───────────────────────────────────────────────────────────────────

def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    try:
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
    except OSError:
        return ""
    return h.hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
