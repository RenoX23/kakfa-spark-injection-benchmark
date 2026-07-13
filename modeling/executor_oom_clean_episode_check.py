"""Robustness check: re-run executor_oom's LOO-CV using only the 3 episodes confirmed
to have genuinely settled (not cold-start-artifact) pre-onset telemetry -- ramptest3,
ramptest7, ramptest10 -- dropping ramptest4/ramptest8 (retained in the first pass, but
their only pre-onset sample was a 2.4MB cold-start reading, not real settled baseline).
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
FEATURE_COLS = ["mean", "std", "min", "max", "last", "n_samples"]
CLEAN_EPISODES = ["ramptest3", "ramptest7", "ramptest10"]


def loo_cv_f1(sub):
    X = sub[FEATURE_COLS].values
    y = sub["label"].values
    groups = sub["episode_id"].values
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
    full = df[df.fault_class == "executor_oom"].reset_index(drop=True)
    full_5ep_f1 = loo_cv_f1(full)
    sub = df[(df.fault_class == "executor_oom") & (df.episode_id.isin(CLEAN_EPISODES))].reset_index(drop=True)
    print("Rows used:")
    print(sub[["episode_id", "window_kind", "label"]].to_string(index=False))
    print()

    X = sub[FEATURE_COLS].values
    y = sub["label"].values
    groups = sub["episode_id"].values

    logo = LeaveOneGroupOut()
    all_true, all_pred = [], []
    per_fold = []
    for train_idx, test_idx in logo.split(X, y, groups):
        scaler = StandardScaler()
        X_train = scaler.fit_transform(X[train_idx])
        X_test = scaler.transform(X[test_idx])
        y_train = y[train_idx]
        held_out = groups[test_idx][0]
        if len(set(y_train)) < 2:
            preds = np.full(len(test_idx), y_train[0])
        else:
            clf = RandomForestClassifier(n_estimators=200, max_depth=5, random_state=42, class_weight="balanced")
            clf.fit(X_train, y_train)
            preds = clf.predict(X_test)
        all_true.extend(y[test_idx].tolist())
        all_pred.extend(preds.tolist())
        per_fold.append({
            "held_out_episode": held_out, "true": y[test_idx].tolist(), "pred": preds.tolist(),
            "train_class_balance": {"normal": int((y_train == 0).sum()), "pre_failure": int((y_train == 1).sum())},
        })

    precision = precision_score(all_true, all_pred, zero_division=0)
    recall = recall_score(all_true, all_pred, zero_division=0)
    f1 = f1_score(all_true, all_pred, zero_division=0)

    result = {
        "clean_episodes_used": CLEAN_EPISODES,
        "n_windows": len(sub),
        "n_episodes": len(CLEAN_EPISODES),
        "class_balance": {"normal": int((y == 0).sum()), "pre_failure": int((y == 1).sum())},
        "precision": precision, "recall": recall, "f1": f1,
        "per_fold": per_fold,
        "comparison_5episode_f1": full_5ep_f1,
    }
    print(f"3-clean-episode LOO-CV: precision={precision:.3f} recall={recall:.3f} f1={f1:.3f}")
    print(f"(for comparison, 5-episode result at current config: f1={full_5ep_f1:.3f})")

    with open(REPO / "results" / "ml-first-pass" / "executor_oom_clean_episode_check.json", "w") as f:
        json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()
