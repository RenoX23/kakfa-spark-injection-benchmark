"""Per-fold breakdown of disk_pressure's std-only LOO-CV (the ablation that produced
F1=0.778, p=0.08) -- is the signal spread across most episodes, or carried by 1-2?
Same data/features/model/CV as magnitude_ablation_check.py, just capturing per-fold
true/pred instead of only the aggregate F1.
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
SHAPE_FEATURES = ["std"]


def main():
    df = pd.read_csv(REPO / "results" / "ml-first-pass" / "extracted_windows.csv")
    sub = df[df.fault_class == "disk_pressure"].reset_index(drop=True)
    X = sub[SHAPE_FEATURES].values
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
        y_test = y[test_idx]
        all_true.extend(y_test.tolist())
        all_pred.extend(preds.tolist())
        n_correct = int((preds == y_test).sum())
        per_fold.append({
            "held_out_episode": held_out,
            "true": y_test.tolist(),
            "pred": preds.tolist(),
            "n_correct": n_correct,
            "n_total": len(y_test),
            "all_correct": n_correct == len(y_test),
        })

    precision = precision_score(all_true, all_pred, zero_division=0)
    recall = recall_score(all_true, all_pred, zero_division=0)
    f1 = f1_score(all_true, all_pred, zero_division=0)

    print(f"disk_pressure std-only: precision={precision:.3f} recall={recall:.3f} f1={f1:.3f}")
    print()
    n_folds_all_correct = sum(1 for f in per_fold if f["all_correct"])
    n_folds_any_error = sum(1 for f in per_fold if not f["all_correct"])
    print(f"folds fully correct: {n_folds_all_correct}/{len(per_fold)}")
    print(f"folds with >=1 error: {n_folds_any_error}/{len(per_fold)}")
    print()
    for fold in per_fold:
        status = "CORRECT" if fold["all_correct"] else "ERROR"
        print(f"  {fold['held_out_episode']:16s} true={fold['true']} pred={fold['pred']} "
              f"({fold['n_correct']}/{fold['n_total']})  {status}")

    result = {
        "precision": precision, "recall": recall, "f1": f1,
        "n_folds_all_correct": n_folds_all_correct,
        "n_folds_any_error": n_folds_any_error,
        "n_folds_total": len(per_fold),
        "per_fold": per_fold,
    }
    with open(REPO / "results" / "ml-first-pass" / "disk_pressure_stdonly_perfold.json", "w") as f:
        json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()
