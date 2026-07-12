"""Follow-up to shuffle_label_check.py: the shuffle test confirms real vs. chance
separation but cannot distinguish genuine fault-precursor signal from the residual
time-of-day/absolute-magnitude drift risk already flagged for the 3 borrowed-reference-
period classes. Feature importance can: if the model relies almost entirely on raw
magnitude (mean/min/max/last) rather than shape (std/n_samples), that's consistent with
exploiting absolute drift level rather than a genuine within-window precursor pattern.
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


def main():
    df = pd.read_csv(REPO / "results" / "ml-first-pass" / "extracted_windows.csv")
    results = {}
    for cls in ["broker_kill", "disk_pressure", "network_degradation", "executor_oom"]:
        sub = df[df.fault_class == cls].reset_index(drop=True)
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
        results[cls] = {"feature_importance_ranked": ranked, "n_folds_averaged": len(importances)}
        print(f"{cls}:")
        for f, imp in ranked:
            print(f"    {f:10s} {imp:.3f}")

    with open(REPO / "results" / "ml-first-pass" / "feature_importance_check.json", "w") as f:
        json.dump(results, f, indent=2)


if __name__ == "__main__":
    main()
