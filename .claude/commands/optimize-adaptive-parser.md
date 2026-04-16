# /optimize-adaptive-parser

Scan `Logs/` for files that the pipeline could not confidently identify, run
`FormatLearner` on each one, display the inferred mappings, and — with user
approval — persist them to `scripts/format_registry.json` so future runs
parse those files automatically without human review.

## What this skill does

1. **Discovers candidate files** — finds every file in `Logs/` (excluding `Logs/audit/`)
   whose last registry entry has `status != SUCCESS` OR whose format is
   `"adaptive_unknown"` OR which has no registry entry at all.

2. **Runs FormatLearner** — for each candidate, calls `FormatLearner.learn()`
   to infer:
   - `structural_type`: `tabular` (CSV/XLSX) · `document` (JSON) · `freetext`
   - `extraction_hint`: `csv_headers` · `json_keys` · `regex_kv`
   - `field_mapping`: raw column/key → canonical schema name
   - `learned_confidence`: fraction of 6 core fields successfully mapped

3. **Displays the proposal** — prints each file's inferred mapping with sample
   values so the user can verify correctness before committing anything.

4. **Asks for approval per file** — prompts `[A]pprove / [R]eject / [S]kip`.
   - `A`: calls `learner.persist(learned)` then `learner.approve(fingerprint)`.
     The entry is saved as `approved: true` in `format_registry.json`.
     Next pipeline run picks it up as Tier 0 and parses without interruption.
   - `R`: calls `learner.persist(learned)` with `approved: false`.
     Stored for reference but will still trigger `DECISION_REQUEST` next run.
   - `S`: nothing written.

5. **Summarises** — reports how many formats were approved, rejected, skipped,
   and prints the current state of `format_registry.json`.

## Format incorrectness — decision reference

Use this table when deciding whether to approve a proposed mapping:

| Condition | What you will see | Correct action |
|---|---|---|
| `learned_confidence >= 0.83` (5–6 core fields mapped) | Green-light proposal | Approve |
| `learned_confidence 0.50–0.83` (3–4 core fields) | Medium-confidence proposal | Verify sample values, then approve or modify |
| `learned_confidence < 0.50` (0–2 core fields mapped) | Weak proposal, many unknowns | Reject or supply manual mapping |
| Mapped field sample values look wrong (e.g. `psp` → numeric IDs) | Mis-mapped column | Reject; file needs a dedicated parser |
| `structural_type = freetext` but file is actually CSV | Wrong type detected | Reject; add a proper CSV header or rename columns |
| File has 0 withdrawal-related lines | FormatLearner returns None | File is not a withdrawal log — exclude from `Logs/` |

## Steps to execute

Run the following Python snippet to perform the full scan-and-learn workflow.
Execute it from the workspace root (`c:/Users/snq9172/Desktop/Log parse/`):

```python
import sys, json
from pathlib import Path
sys.path.insert(0, ".")

from scripts.format_learner import FormatLearner
from scripts.registry import Registry

LOGS_DIR    = Path("Logs")
SCRIPTS_DIR = Path("scripts")

learner  = FormatLearner(SCRIPTS_DIR)
registry = Registry(SCRIPTS_DIR)

# ── 1. find candidate files ──────────────────────────────────────────────────
supported = {".txt", ".csv", ".json", ".jsonl", ".log", ".xlsx"}
candidates = []
for fp in sorted(LOGS_DIR.rglob("*")):
    if fp.is_file() and fp.suffix.lower() in supported and "audit" not in fp.parts:
        should, attempt = registry.needs_processing(fp)
        if should:
            candidates.append(fp)
        else:
            # Also check if it was parsed as adaptive_unknown
            matches = [e for e in registry.all_entries() if e["file_name"] == fp.name]
            if matches and matches[-1].get("format") == "adaptive_unknown":
                candidates.append(fp)

print(f"Candidates: {len(candidates)} file(s)\n")

# ── 2. learn + present + approve ────────────────────────────────────────────
approved_count = rejected_count = skipped_count = 0

for fp in candidates:
    print(f"{'='*60}")
    print(f"File : {fp.name}")

    learned = learner.learn(fp)
    if learned is None:
        print("  FormatLearner returned None — no withdrawal data found.")
        print("  This file may not be a withdrawal log.\n")
        skipped_count += 1
        continue

    print(learned.summary())
    print()
    print("Sample values per mapped field:")
    for raw_col, canonical in learned.field_mapping.items():
        vals = learned.sample_values.get(raw_col, [])
        print(f"  {canonical:<14} <- {raw_col!r:25s}  samples={vals}")
    print()

    while True:
        choice = input("  [A]pprove / [R]eject / [S]kip: ").strip().upper()
        if choice in ("A", "R", "S"):
            break

    if choice == "A":
        learner.persist(learned)
        learner.approve(learned.fingerprint)
        print(f"  Approved and saved. Fingerprint: {learned.fingerprint[:16]}...")
        approved_count += 1
    elif choice == "R":
        learner.persist(learned)
        print(f"  Rejected and saved (approved=false).")
        rejected_count += 1
    else:
        print("  Skipped — nothing written.")
        skipped_count += 1

# ── 3. summary ───────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"  DONE: {approved_count} approved  |  {rejected_count} rejected  |  {skipped_count} skipped")

registry_path = SCRIPTS_DIR / "format_registry.json"
if registry_path.exists():
    reg = json.loads(registry_path.read_text())
    print(f"\n  format_registry.json — {len(reg)} total entries:")
    for fp_hash, entry in reg.items():
        badge = "APPROVED" if entry.get("approved") else "pending"
        print(f"    [{badge}]  {entry['format_name']}  conf={entry['learned_confidence']:.2f}"
              f"  seen={entry['times_seen']}x  files={entry['file_examples']}")
```

After at least one format is approved, re-run the pipeline:

```bash
python scripts/run_pipeline.py --all
```

Files whose fingerprint now has `approved: true` in `format_registry.json` will
be parsed via Tier 0 (registry lookup) — no `DECISION_REQUEST`, no human pause.

## When to re-run this skill

- A new `.csv`, `.txt`, `.json`, or `.xlsx` file is dropped into `Logs/` and
  the pipeline emits a `DECISION_REQUEST` for it.
- A file consistently produces high `records_rejected` counts in the audit log.
- `results/report_*.md` shows `source_format: adaptive_unknown` for a file you
  expect to be reliably parseable.
- After modifying column names in a data source (fingerprint changes → old
  approved entry no longer matches → new learning run needed).
