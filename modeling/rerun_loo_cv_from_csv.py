"""Re-run LOO-CV (full feature set) directly from the current extracted_windows.csv,
without re-querying Prometheus. Used to re-score network_degradation and broker_kill
after their rows were replaced by reextract_class.py (fixed normal-reference stride),
without touching disk_pressure/executor_oom's already-current rows.

Usage: .venv/bin/python modeling/rerun_loo_cv_from_csv.py <class1> [class2 ...]
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pandas as pd
from extract_and_train import loo_cv_train_eval

REPO = Path(__file__).resolve().parent.parent


def main():
    classes = sys.argv[1:]
    df = pd.read_csv(REPO / "results" / "ml-first-pass" / "extracted_windows.csv")

    results_path = REPO / "results" / "ml-first-pass" / "loo_cv_results.json"
    results = json.loads(results_path.read_text())

    for cls in classes:
        sub = df[df.fault_class == cls].reset_index(drop=True)
        res = loo_cv_train_eval(sub, cls)
        results[cls] = res
        print(f"{cls}: precision={res['precision']:.3f} recall={res['recall']:.3f} f1={res['f1']:.3f} "
              f"n_windows={res['n_windows_total']} n_episodes={res['n_episodes']}")

    with open(results_path, "w") as f:
        json.dump(results, f, indent=2, default=str)


if __name__ == "__main__":
    main()
