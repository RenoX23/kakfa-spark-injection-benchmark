"""Gate-audit finding (Weeks 10-11, 2026-07-13): the mechanism trace behind disk_pressure's
lead-time reconstruction asserted a per-instance ablation result ("every fold's decision
boundary sits at ~-350,000 bytes, negligible cross-fold variation") from prose, with no
committed script or output -- a real gap against this project's own "every number
traceable to a committed JSON" standard. This persists that ablation properly.

For each of the 7 clean episodes' held-out LOO delta-feature models (disk_pressure_
lead_time.py), sweeps the delta_mean value (with its three collinear siblings --
delta_min/max/last are identical to delta_mean at every real single-sample window this
pass encountered, since mean=min=max=last when n_samples=1) through a fine grid, holding
std=0 and n_samples=1 fixed (both fixed at their observed values throughout every
detection window in this pass), to find the exact boundary where each fold's prediction
flips from pre_failure (1) to normal (0). Confirms whether that boundary is a static
absolute-value-proximity check (consistent across folds, no relation to a physical
escalation trend) or something more nuanced.
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import LeaveOneGroupOut
from sklearn.preprocessing import StandardScaler

REPO = Path(__file__).resolve().parent.parent

DELTA_FEATURE_COLS = ["delta_mean", "delta_min", "delta_max", "delta_last", "std", "n_samples"]
EPISODE_BASELINES = {
    "campaign1": 995152609280.0, "campaign2": 995152027648.0, "campaign3": 995150802944.0,
    "campaign4": 995149934592.0, "campaign5": 995148972032.0, "campaign7": 995146944512.0,
    "campaign8": 995145748480.0, "topup1": 995050299392.0,
}
QUIET_PERIOD_MEAN = 994899156269.1765

CLEAN_EPISODES = {
    "campaign1": 684032.0, "campaign2": 0.0, "campaign3": 438272.0, "campaign4": 376832.0,
    "campaign5": 204800.0, "campaign7": 360448.0, "campaign8": 368640.0,
}
SWEEP_START, SWEEP_END, SWEEP_STEP = 700_000, -2_200_000, -5_000  # fine grid, 5KB resolution


def main():
    full_df = pd.read_csv(REPO / "results" / "ml-first-pass" / "extracted_windows.csv")
    sub = full_df[full_df.fault_class == "disk_pressure"].reset_index(drop=True)
    baselines = sub["episode_id"].map(lambda e: EPISODE_BASELINES.get(e, QUIET_PERIOD_MEAN))
    sub["delta_mean"] = sub["mean"] - baselines
    sub["delta_min"] = sub["min"] - baselines
    sub["delta_max"] = sub["max"] - baselines
    sub["delta_last"] = sub["last"] - baselines
    X = sub[DELTA_FEATURE_COLS].values
    y = sub["label"].values
    groups = sub["episode_id"].values
    logo = LeaveOneGroupOut()

    results = {}
    for train_idx, test_idx in logo.split(X, y, groups):
        held_out = groups[test_idx][0]
        if held_out not in CLEAN_EPISODES:
            continue
        scaler = StandardScaler()
        X_train = scaler.fit_transform(X[train_idx])
        y_train = y[train_idx]
        clf = RandomForestClassifier(n_estimators=200, max_depth=5, random_state=42, class_weight="balanced")
        clf.fit(X_train, y_train)

        flip_threshold = None
        for delta in range(SWEEP_START, SWEEP_END, SWEEP_STEP):
            instance = np.array([[delta, delta, delta, delta, 0.0, 1.0]])
            pred = int(clf.predict(scaler.transform(instance))[0])
            if pred == 0:
                flip_threshold = delta
                break

        actual_delta = CLEAN_EPISODES[held_out]
        margin = actual_delta - flip_threshold if flip_threshold is not None else None
        results[held_out] = {
            "actual_delta_at_detection": actual_delta,
            "fold_flip_threshold": flip_threshold,
            "margin_above_threshold": margin,
            "sweep_resolution_bytes": abs(SWEEP_STEP),
        }
        print(f"{held_out:10s} actual_delta={actual_delta:>10,.0f}  "
              f"fold_flip_threshold={flip_threshold:>10,.0f}  margin={margin:>10,.0f}")

    thresholds = [r["fold_flip_threshold"] for r in results.values()]
    summary = {
        "mean_threshold": float(np.mean(thresholds)),
        "std_threshold": float(np.std(thresholds)),
        "min_threshold": float(np.min(thresholds)),
        "max_threshold": float(np.max(thresholds)),
    }
    print(f"\ncross-fold threshold: mean={summary['mean_threshold']:,.0f} "
          f"std={summary['std_threshold']:,.0f} range=[{summary['min_threshold']:,.0f}, "
          f"{summary['max_threshold']:,.0f}]")

    out = {"per_fold": results, "cross_fold_summary": summary}
    with open(REPO / "results" / "ml-first-pass" / "disk_pressure_lead_time_ablation.json", "w") as f:
        json.dump(out, f, indent=2)


if __name__ == "__main__":
    main()
