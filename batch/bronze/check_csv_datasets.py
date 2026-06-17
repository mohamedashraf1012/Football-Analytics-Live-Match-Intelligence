"""
check_csv_datasets.py
---------------------
Quick inspection of the 12 Transfermarkt raw CSV files before upload.
Prints shape, dtypes, null counts, and a 3-row sample for each file.
"""

import os
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
DATA_DIR = "./data"          # folder that holds all 12 CSVs (change if needed)

CSV_FILES = [
    "players.csv",
    "clubs.csv",
    "competitions.csv",
    "transfers.csv",
    "matches.csv",
    "game_events.csv",
    "appearances.csv",
    "club_games.csv",
    "player_valuations.csv",
    "games.csv",
    "game_lineups.csv",
    "game_stats.csv",
]
# ─────────────────────────────────────────────────────────────────────────────


def inspect(path: str, name: str) -> None:
    print(f"\n{'='*60}")
    print(f"  FILE : {name}")
    print(f"{'='*60}")

    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception as exc:
        print(f"  ❌  Could not read file: {exc}")
        return

    rows, cols = df.shape
    null_cols   = df.isnull().sum()
    null_cols   = null_cols[null_cols > 0]

    print(f"  Shape   : {rows:,} rows  ×  {cols} columns")
    print(f"  Columns : {list(df.columns)}")
    print(f"\n  Dtypes:\n{df.dtypes.to_string()}")

    if null_cols.empty:
        print("\n  Nulls   : ✅  No nulls found")
    else:
        print(f"\n  Nulls (columns with missing values):\n{null_cols.to_string()}")

    print(f"\n  Sample (3 rows):\n{df.head(3).to_string(index=False)}")


def main() -> None:
    print("\n🔍  Transfermarkt CSV Quick Inspection")
    print(f"    Data directory : {os.path.abspath(DATA_DIR)}\n")

    found, missing = [], []
    for fname in CSV_FILES:
        fpath = os.path.join(DATA_DIR, fname)
        if os.path.isfile(fpath):
            found.append((fpath, fname))
        else:
            missing.append(fname)

    print(f"  Files found   : {len(found)} / {len(CSV_FILES)}")
    if missing:
        print(f"  Files missing : {missing}")

    for fpath, fname in found:
        inspect(fpath, fname)

    print(f"\n\n✅  Inspection complete — {len(found)} file(s) scanned.")


if __name__ == "__main__":
    main()
