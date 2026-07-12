"""Label-permutation sanity check for the 3 borrowed-reference-period classes
(broker_kill, disk_pressure, network_degradation). Same features/groups/model/CV as
modeling/extract_and_train.py -- only y is randomly permuted. Run N_SHUFFLES times
(a single shuffle is a noisy point estimate) and report the resulting F1 distribution
against the real, unshuffled F1.
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import LeaveOneGroupOut
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import f1_score

REPO = Path(__file__).resolve().parent.parent
FEATURE_COLS = ["mean", "std", "min", "max", "last", "n_samples"]
N_SHUFFLES = 30
SEED = 42


def loo_cv_f1(X, y, groups):
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
            clf = RandomForestClassifier(n_estimators=200, max_depth=5, random_state=42, class_weight="balanced")
            clf.fit(X_train, y_train)
            preds = clf.predict(X_test)
        all_true.extend(y[test_idx].tolist())
        all_pred.extend(preds.tolist())
    return f1_score(all_true, all_pred, zero_division=0)


def main():
    df = pd.read_csv(REPO / "results" / "ml-first-pass" / "extracted_windows.csv")
    rng = np.random.default_rng(SEED)
    results = {}

    for cls in ["broker_kill", "disk_pressure", "network_degradation"]:
        sub = df[df.fault_class == cls].reset_index(drop=True)
        X = sub[FEATURE_COLS].values
        y_true = sub["label"].values
        groups = sub["episode_id"].values

        real_f1 = loo_cv_f1(X, y_true, groups)

        shuffled_f1s = []
        for i in range(N_SHUFFLES):
            y_shuffled = rng.permutation(y_true)
            f1 = loo_cv_f1(X, y_shuffled, groups)
            shuffled_f1s.append(f1)

        results[cls] = {
            "real_f1": real_f1,
            "n_windows": len(sub),
            "class_balance": {"normal": int((y_true == 0).sum()), "pre_failure": int((y_true == 1).sum())},
            "shuffled_f1_mean": float(np.mean(shuffled_f1s)),
            "shuffled_f1_std": float(np.std(shuffled_f1s)),
            "shuffled_f1_min": float(np.min(shuffled_f1s)),
            "shuffled_f1_max": float(np.max(shuffled_f1s)),
            "shuffled_f1_all": shuffled_f1s,
            "n_shuffles": N_SHUFFLES,
        }
        print(f"{cls}: real_f1={real_f1:.3f}  shuffled_f1 mean={np.mean(shuffled_f1s):.3f} "
              f"std={np.std(shuffled_f1s):.3f} range=[{np.min(shuffled_f1s):.3f},{np.max(shuffled_f1s):.3f}] "
              f"(n_shuffles={N_SHUFFLES})")

    out_dir = REPO / "results" / "ml-first-pass"
    with open(out_dir / "shuffle_label_check.json", "w") as f:
        json.dump(results, f, indent=2)


if __name__ == "__main__":
    main()
