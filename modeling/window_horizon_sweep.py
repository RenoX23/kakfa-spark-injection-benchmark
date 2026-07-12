"""Closes the second accepted-limitation gap from the Weeks 8-9 gate-audit (Section 8):
a real window/horizon sensitivity sweep, not a single corrected-then-fixed value.

Grid bounded by REAL measured inter-episode gaps (modeling/extract_and_train.py's
[15s,30s]/30s default needs max(horizons)+window=60s of lookback -- computed against
the actual ground-truth timestamps this run, the tightest real gap in EVERY class is
below that: broker_kill 47s, executor_oom 32-33s (6 of 7 gaps), disk_pressure 49s,
network_degradation 54s. That means the already-shipped, already-gate-audited default
has at least one contamination-risk pre-failure window per class -- the label window
reaches back far enough to overlap the PREVIOUS episode's fault/recovery period, not
just genuinely quiet pre-onset time. Not previously caught because the [15,30]/30 fix
only had to clear "does a real sample exist here at all" (the collapse bug), not
"is this window free of the neighboring episode's own fault," a different question.

Grid, from safest to the current default (each point's per-class safety reported, not
assumed):
  G0: window=10s, horizons=[5,10]   -- needs 20s, safe for every real gap in this dataset
  G1: window=15s, horizons=[10,15]  -- needs 30s, safe for every real gap (min margin 2s,
                                        executor_oom's 32s pairs)
  G2: window=20s, horizons=[10,20]  -- needs 40s, safe for broker_kill/disk_pressure/
                                        network_degradation, NOT for executor_oom (32-33s)
  G3: window=30s, horizons=[15,30]  -- needs 60s (current shipped default), unsafe for
                                        >=1 gap in every class -- included as the reference
                                        point every other Section 6.5 result was built on

Fixed, untuned RandomForestClassifier(n_estimators=200, max_depth=5,
class_weight="balanced") throughout -- isolates window/horizon's effect on F1 without
conflating it with per-grid-point hyperparameter search noise (that question belongs to
multi_model_nested_tuning.py, which holds window/horizon fixed and varies model/tuning
instead). Extracted data is held in memory only, per grid point -- this script never
writes results/ml-first-pass/extracted_windows.csv, which stays exactly as it was for
the already gate-audited G3 result.
"""
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import LeaveOneGroupOut
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import precision_score, recall_score, f1_score

import extract_and_train as eat

REPO = Path(__file__).resolve().parent.parent
CAMPAIGN_DIR = REPO / "results" / "campaign-n8"

GRID = [
    {"name": "G0", "window_s": 10, "horizons_s": [5, 10]},
    {"name": "G1", "window_s": 15, "horizons_s": [10, 15]},
    {"name": "G2", "window_s": 20, "horizons_s": [10, 20]},
    {"name": "G3_current_default", "window_s": 30, "horizons_s": [15, 30]},
]


def iso(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def min_real_gap_s(cls):
    cfg = eat.CLASS_CONFIG[cls]
    files = sorted((CAMPAIGN_DIR / cfg["dir"]).glob("*.json"))
    eps = [json.loads(f.read_text()) for f in files]
    eps.sort(key=lambda e: e[cfg["onset_field"]])
    gaps = []
    for i in range(1, len(eps)):
        prev_end = eps[i - 1].get(cfg["end_field"]) or eps[i - 1][cfg["onset_field"]]
        gaps.append((iso(eps[i][cfg["onset_field"]]) - iso(prev_end)).total_seconds())
    return min(gaps) if gaps else None


def fixed_rf_loo_cv(df):
    X = df[["mean", "std", "min", "max", "last", "n_samples"]].values
    y = df["label"].values
    groups = df["episode_id"].values
    logo = LeaveOneGroupOut()
    all_true, all_pred = [], []
    for train_idx, test_idx in logo.split(X, y, groups):
        scaler = StandardScaler()
        X_train = scaler.fit_transform(X[train_idx])
        X_test = scaler.transform(X[test_idx])
        y_train = y[train_idx]
        if len(set(y_train)) < 2:
            preds = np.full(len(test_idx), y_train[0])
        else:
            clf = RandomForestClassifier(n_estimators=200, max_depth=5, random_state=42,
                                          class_weight="balanced", n_jobs=1)
            clf.fit(X_train, y_train)
            preds = clf.predict(X_test)
        all_true.extend(y[test_idx].tolist())
        all_pred.extend(preds.tolist())
    precision = precision_score(all_true, all_pred, zero_division=0)
    recall = recall_score(all_true, all_pred, zero_division=0)
    f1 = f1_score(all_true, all_pred, zero_division=0)
    return {"precision": precision, "recall": recall, "f1": f1,
            "n_windows": len(df), "n_groups": df["episode_id"].nunique()}


def main():
    min_gaps = {cls: min_real_gap_s(cls) for cls in eat.CLASS_CONFIG}
    print("Real minimum inter-episode gap per class:", min_gaps, flush=True)

    prom = eat.PromClient()
    results = {"min_real_gap_s": min_gaps, "grid": {}}
    try:
        for point in GRID:
            eat.HORIZONS_S = point["horizons_s"]
            eat.WINDOW_S = point["window_s"]
            eat.NORMAL_REF_WINDOW_S = eat.WINDOW_S
            needed_lookback = max(point["horizons_s"]) + point["window_s"]

            print(f"=== {point['name']}: window={point['window_s']}s horizons={point['horizons_s']} "
                  f"(needs {needed_lookback}s lookback) ===", flush=True)
            point_result = {"window_s": point["window_s"], "horizons_s": point["horizons_s"],
                             "needed_lookback_s": needed_lookback, "per_class": {}}
            for cls in eat.CLASS_CONFIG:
                df = eat.extract_class(cls, prom)
                metrics = fixed_rf_loo_cv(df)
                contamination_safe = needed_lookback <= min_gaps[cls]
                metrics["contamination_safe"] = contamination_safe
                metrics["min_real_gap_s"] = min_gaps[cls]
                point_result["per_class"][cls] = metrics
                safe_str = "SAFE" if contamination_safe else "AT RISK"
                print(f"  {cls}: f1={metrics['f1']:.3f} precision={metrics['precision']:.3f} "
                      f"recall={metrics['recall']:.3f} n_windows={metrics['n_windows']} "
                      f"({safe_str}: needs {needed_lookback}s, min real gap {min_gaps[cls]:.0f}s)", flush=True)
            results["grid"][point["name"]] = point_result
    finally:
        prom.close()
        # restore the module defaults so nothing downstream that imports this module
        # mid-process inherits a swept config
        eat.HORIZONS_S = [15, 30]
        eat.WINDOW_S = 30
        eat.NORMAL_REF_WINDOW_S = eat.WINDOW_S

    with open(REPO / "results" / "ml-first-pass" / "window_horizon_sweep.json", "w") as f:
        json.dump(results, f, indent=2)
    print("Done. extracted_windows.csv was NOT modified by this script.", flush=True)


if __name__ == "__main__":
    main()
