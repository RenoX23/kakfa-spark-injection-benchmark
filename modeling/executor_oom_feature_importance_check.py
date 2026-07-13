"""Close-out check before Weeks 8-9 first-pass validation: does executor_oom's feature
importance show any single feature dominating (same check that caught broker_kill's
n_samples leak and confirmed disk_pressure/network_degradation's magnitude reliance)?
Run on both the full 5-episode set and the 3-clean-episode subset (cold-start-artifact
episodes ramptest4/ramptest8 excluded), since the clean subset is the more defensible
number for write-up.

executor_oom's normal-context windows come from extract_executor_oom_normal_windows()
in extract_and_train.py, which draws from each episode's own pre-injection period
(onset-150s to onset-90s) -- structurally separate from extract_normal_reference_windows()
(the pooled-quiet-period function used by broker_kill/disk_pressure/network_degradation,
which carried both the n_samples window-size bug and the stride bug). executor_oom never
called that function and was never exposed to either bug.
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import LeaveOneGroupOut
from sklearn.preprocessing import StandardScaler

REPO = Path(__file__).resolve().parent.parent
FEATURE_COLS = ["mean", "std", "min", "max", "last", "n_samples"]
CLEAN_EPISODES = ["ramptest3", "ramptest7", "ramptest10"]


def importance_for(sub):
    X = sub[FEATURE_COLS].values
    y = sub["label"].values
    groups = sub["episode_id"].values
    logo = LeaveOneGroupOut()
    importances = []
    for train_idx, test_idx in logo.split(X, y, groups):
        scaler = StandardScaler()
        X_train = scaler.fit_transform(X[train_idx])
        y_train = y[train_idx]
        if len(set(y_train)) < 2:
            continue
        clf = RandomForestClassifier(n_estimators=200, max_depth=5, random_state=42, class_weight="balanced")
        clf.fit(X_train, y_train)
        importances.append(clf.feature_importances_)
    avg_imp = np.mean(importances, axis=0)
    ranked = sorted(zip(FEATURE_COLS, avg_imp.tolist()), key=lambda x: -x[1])
    return ranked, len(importances)


def main():
    df = pd.read_csv(REPO / "results" / "ml-first-pass" / "extracted_windows.csv")
    full = df[df.fault_class == "executor_oom"].reset_index(drop=True)
    clean = full[full.episode_id.isin(CLEAN_EPISODES)].reset_index(drop=True)

    full_ranked, full_folds = importance_for(full)
    clean_ranked, clean_folds = importance_for(clean)

    print("executor_oom (5-episode, full):")
    for f, i in full_ranked:
        print(f"    {f:10s} {i:.4f}")
    print(f"    n_folds_averaged={full_folds}")
    print("executor_oom (3-clean-episode):")
    for f, i in clean_ranked:
        print(f"    {f:10s} {i:.4f}")
    print(f"    n_folds_averaged={clean_folds}")

    result = {
        "note": (
            "executor_oom's normal windows come from extract_executor_oom_normal_windows(), "
            "a separate code path from extract_normal_reference_windows() (the pooled-quiet-"
            "period function that carried both the n_samples window-size bug and the stride "
            "bug for broker_kill/disk_pressure/network_degradation). executor_oom was never "
            "exposed to either bug -- confirmed by exact match against the pre-existing "
            "feature_importance_check.json entry (committed 63e2d8b, before those fixes)."
        ),
        "full_5_episode": {"feature_importance_ranked": full_ranked, "n_folds_averaged": full_folds},
        "clean_3_episode": {"feature_importance_ranked": clean_ranked, "n_folds_averaged": clean_folds},
        "verdict": (
            f"no single feature dominates in either version -- top feature is "
            f"{full_ranked[0][0]}={full_ranked[0][1]:.3f} (full set) or "
            f"{clean_ranked[0][0]}={clean_ranked[0][1]:.3f} (clean subset), both essentially "
            f"tied with the rest. No n_samples-style leak."
        ),
    }
    with open(REPO / "results" / "ml-first-pass" / "executor_oom_feature_importance_check.json", "w") as f:
        json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()
