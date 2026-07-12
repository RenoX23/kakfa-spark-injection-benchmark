"""Re-extract a single fault class using the fixed normal-reference stride (spread
across the full 67-minute quiet period instead of packed into its first 5 minutes,
which overlapped executor_oom's ramptest3 fault window -- see extract_and_train.py's
NORMAL_REF_STRIDE_S comment for the full bug history). Replaces that class's rows in
the committed extracted_windows.csv; all other classes' rows are left untouched.

Usage: .venv/bin/python modeling/reextract_class.py <fault_class>
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pandas as pd
from extract_and_train import PromClient, extract_class, CLASS_CONFIG

REPO = Path(__file__).resolve().parent.parent


def main():
    cls = sys.argv[1]
    if cls not in CLASS_CONFIG:
        raise SystemExit(f"unknown class {cls!r}, expected one of {list(CLASS_CONFIG)}")

    prom = PromClient()
    try:
        new_df = extract_class(cls, prom)
    finally:
        prom.close()

    print(f"Re-extracted {cls}: {len(new_df)} windows")
    print(new_df[["episode_id", "window_kind", "label"]].to_string(index=False))

    csv_path = REPO / "results" / "ml-first-pass" / "extracted_windows.csv"
    full_df = pd.read_csv(csv_path)
    full_df = full_df[full_df.fault_class != cls]
    full_df = pd.concat([full_df, new_df], ignore_index=True)
    full_df.to_csv(csv_path, index=False)
    print(f"\nUpdated {csv_path} -- {cls} rows replaced, other classes untouched.")


if __name__ == "__main__":
    main()
