"""
Polling data cleanup script.

Deletes accumulated polling/runtime data:
  - data/backtests/   — backtest result JSON files
  - data/news/        — news CSV files
  - data/history/     — historical OHLC CSV files
  - data/features/    — AI-extracted feature JSON files
  - data/indicators/  — computed indicator CSV files  (opt-in)
  - data/realtime/    — real-time snapshot CSV files  (opt-in)

Usage:
    python scripts/clean_polling_data.py [--all] [--dry-run]

Flags:
    --all       Also wipe indicators/ and realtime/ (heavier re-compute cost)
    --dry-run   List files that would be deleted without actually deleting them
"""

import argparse
import os
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _count_files(folder: Path) -> int:
    if not folder.exists():
        return 0
    return sum(1 for f in folder.iterdir() if f.is_file())


def _delete_folder_contents(folder: Path, dry_run: bool) -> int:
    """Delete all files directly inside *folder* (non-recursive). Returns count."""
    if not folder.exists():
        print(f"  [skip] {folder} — does not exist")
        return 0

    files = [f for f in folder.iterdir() if f.is_file()]
    if not files:
        print(f"  [empty] {folder}")
        return 0

    for f in files:
        if dry_run:
            print(f"  [dry-run] would delete {f.name}")
        else:
            f.unlink()

    verb = "would delete" if dry_run else "deleted"
    print(f"  [{verb}] {len(files)} file(s) in {folder}")
    return len(files)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Clean polling/runtime data folders.")
    parser.add_argument("--all", action="store_true",
                        help="Also clean indicators/ and realtime/ folders.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be deleted without deleting anything.")
    args = parser.parse_args()

    # Resolve project root (two levels up from this script: scripts/ → project root)
    project_root = Path(__file__).resolve().parent.parent
    data_dir = project_root / "data"

    if not data_dir.exists():
        print(f"ERROR: data directory not found at {data_dir}", file=sys.stderr)
        sys.exit(1)

    # Core targets (always cleaned)
    targets = {
        "backtest results":  data_dir / "backtests",
        "news data":         data_dir / "news",
        "historical data":   data_dir / "history",
        "AI features":       data_dir / "features",
    }

    # Optional targets (only with --all)
    if args.all:
        targets["indicators"] = data_dir / "indicators"
        targets["realtime"]   = data_dir / "realtime"

    if args.dry_run:
        print("=== DRY RUN — no files will be deleted ===\n")

    total = 0
    for label, folder in targets.items():
        print(f"Cleaning {label} ({folder.relative_to(project_root)}):")
        total += _delete_folder_contents(folder, dry_run=args.dry_run)
        print()

    verb = "would be deleted" if args.dry_run else "deleted"
    print(f"Done. {total} file(s) {verb} in total.")


if __name__ == "__main__":
    main()
