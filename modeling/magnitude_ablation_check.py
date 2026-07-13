"""Cheap ablation: does any real precursor signal survive for disk_pressure/
network_degradation once raw-magnitude features (mean, min, max, last) are dropped,
leaving only shape/dispersion features? No trend/slope feature exists in the current
extraction (only mean/std/min/max/last/n_samples were computed), so the surviving
feature set is std alone. n_samples is also excluded -- it's not a magnitude feature,
but it's not a shape feature either (already confirmed near-zero importance for these
two classes in feature_importance_check.py, so excluding it doesn't cost anything real).
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
SHAPE_FEATURES = ["std"]
FULL_FEATURES = ["mean", "std", "min", "max", "last", "n_samples"]
N_SHUFFLES = 100  # raised from 30: comparing real F1 against the single observed max of
# a 30-sample null distribution is not a real significance test -- a proper permutation
# p-value is (count of shuffled F1 >= real F1) / n_shuffles, which needs more resolution
# than 30 draws gives at F1's coarse granularity for this sample size.
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

    for cls in ["disk_pressure", "network_degradation"]:
        sub = df[df.fault_class == cls].reset_index(drop=True)
        y_true = sub["label"].values
        groups = sub["episode_id"].values

        full_feature_real_f1 = loo_cv_f1(sub[FULL_FEATURES].values, y_true, groups)
        X = sub[SHAPE_FEATURES].values
        real_f1 = loo_cv_f1(X, y_true, groups)
        shuffled_f1s = [loo_cv_f1(X, rng.permutation(y_true), groups) for _ in range(N_SHUFFLES)]

        n_shuffled_ge_real = sum(1 for f in shuffled_f1s if f >= real_f1)
        p_value = n_shuffled_ge_real / N_SHUFFLES

        results[cls] = {
            "features_used": SHAPE_FEATURES,
            "n_shuffles": N_SHUFFLES,
            "real_f1": real_f1,
            "shuffled_f1_mean": float(np.mean(shuffled_f1s)),
            "shuffled_f1_std": float(np.std(shuffled_f1s)),
            "shuffled_f1_min": float(np.min(shuffled_f1s)),
            "shuffled_f1_max": float(np.max(shuffled_f1s)),
            "n_shuffled_ge_real": n_shuffled_ge_real,
            "p_value": p_value,
            "full_feature_real_f1_for_comparison": full_feature_real_f1,
        }
        print(f"{cls} (std only): real_f1={real_f1:.3f}  shuffled mean={np.mean(shuffled_f1s):.3f} "
              f"std={np.std(shuffled_f1s):.3f} range=[{np.min(shuffled_f1s):.3f},{np.max(shuffled_f1s):.3f}]")
        print(f"    {n_shuffled_ge_real}/{N_SHUFFLES} shuffled F1 >= real F1  ->  p={p_value:.3f}")
        print(f"    (full-feature-set real F1 was {results[cls]['full_feature_real_f1_for_comparison']:.3f})")

    with open(REPO / "results" / "ml-first-pass" / "magnitude_ablation_check.json", "w") as f:
        json.dump(results, f, indent=2)


if __name__ == "__main__":
    main()
