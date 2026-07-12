"""Re-extract disk_pressure only, using the fixed normal-reference stride (spread across
the full 67-minute quiet period instead of packed into its first 5 minutes, which
overlapped executor_oom's ramptest3 fault window). Replaces disk_pressure's rows in the
committed extracted_windows.csv; leaves broker_kill/network_degradation/executor_oom
untouched -- not in scope for this diagnostic.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pandas as pd
from extract_and_train import PromClient, extract_class

REPO = Path(__file__).resolve().parent.parent


def main():
    prom = PromClient()
    try:
        new_disk_df = extract_class("disk_pressure", prom)
    finally:
        prom.close()

    print(f"Re-extracted disk_pressure: {len(new_disk_df)} windows")
    print(new_disk_df[["episode_id", "window_kind", "label"]].to_string(index=False))

    csv_path = REPO / "results" / "ml-first-pass" / "extracted_windows.csv"
    full_df = pd.read_csv(csv_path)
    full_df = full_df[full_df.fault_class != "disk_pressure"]
    full_df = pd.concat([full_df, new_disk_df], ignore_index=True)
    full_df.to_csv(csv_path, index=False)
    print(f"\nUpdated {csv_path} -- disk_pressure rows replaced, other classes untouched.")


if __name__ == "__main__":
    main()
