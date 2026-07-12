"""broker_kill's full-feature LOO-CV (post stride-fix) jumped from F1=0.476 to F1=0.941,
but feature-importance inspection shows n_samples alone carries 0.969 of total importance
-- mean/std/min/max/last are flat (1.0/0.0) across nearly every window because the up{}
metric barely moves in a 30s window. This is the same class of artifact as the earlier
n_samples window-size leak (session bug #2): pre-failure windows (built from a query
starting well before onset) mostly land on n_samples=3, normal-reference windows (built
from a query grid that doesn't align with each window's stride offset) mostly land on
n_samples=2 -- a sampling-density parity difference, not a broker-health signal.

This checks whether any real signal survives once n_samples is dropped, same discipline
as the disk_pressure/network_degradation magnitude ablation.
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import LeaveOneGroupOut
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import precision_score, recall_score, f1_score

REPO = Path(__file__).resolve().parent.parent
FEATURES_NO_NSAMPLES = ["mean", "std", "min", "max", "last"]
N_SHUFFLES = 100
SEED = 42


def loo_cv_eval(X, y, groups):
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
    return all_true, all_pred


def main():
    df = pd.read_csv(REPO / "results" / "ml-first-pass" / "extracted_windows.csv")
    sub = df[df.fault_class == "broker_kill"].reset_index(drop=True)
    X = sub[FEATURES_NO_NSAMPLES].values
    y = sub["label"].values
    groups = sub["episode_id"].values

    true, pred = loo_cv_eval(X, y, groups)
    precision = precision_score(true, pred, zero_division=0)
    recall = recall_score(true, pred, zero_division=0)
    f1 = f1_score(true, pred, zero_division=0)

    rng = np.random.default_rng(SEED)
    shuffled_f1s = []
    for _ in range(N_SHUFFLES):
        y_shuffled = rng.permutation(y)
        t, p = loo_cv_eval(X, y_shuffled, groups)
        shuffled_f1s.append(f1_score(t, p, zero_division=0))
    n_ge = sum(1 for f in shuffled_f1s if f >= f1)
    p_value = n_ge / N_SHUFFLES

    print(f"broker_kill (no n_samples): precision={precision:.3f} recall={recall:.3f} f1={f1:.3f}")
    print(f"shuffled F1: mean={np.mean(shuffled_f1s):.3f} range=[{np.min(shuffled_f1s):.3f},{np.max(shuffled_f1s):.3f}]")
    print(f"{n_ge}/{N_SHUFFLES} shuffled F1 >= real F1  ->  p={p_value:.3f}")

    result = {
        "features_used": FEATURES_NO_NSAMPLES,
        "precision": precision, "recall": recall, "f1": f1,
        "n_shuffles": N_SHUFFLES,
        "shuffled_f1_mean": float(np.mean(shuffled_f1s)),
        "shuffled_f1_std": float(np.std(shuffled_f1s)),
        "n_shuffled_ge_real": n_ge, "p_value": p_value,
        "comparison_full_features_incl_nsamples_f1": 0.9411764705882353,
        "comparison_full_features_incl_nsamples_n_samples_importance": 0.969,
    }
    with open(REPO / "results" / "ml-first-pass" / "broker_kill_no_nsamples_check.json", "w") as f:
        json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()
