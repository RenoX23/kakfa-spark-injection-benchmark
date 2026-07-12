"""Gate-auditor finding (2026-07-12): executor_oom was the only one of 4 classes to reach
a "positive" verdict without a permutation/null test, and its F1 sits BELOW a trivial
always-predict-pre_failure baseline (0.842 vs ~0.900 for the 5-episode set; 0.727 vs
~0.833 for the 3-clean-episode set) -- the model also misclassifies both normal windows
in every configuration tried (0/2 specificity). Same rigor as broker_kill/disk_pressure/
network_degradation: majority-class trivial baseline + rank-based shuffle test, on both
the 5-episode and 3-clean-episode subsets.
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


def trivial_majority_f1(y):
    majority = 1 if (y == 1).sum() >= (y == 0).sum() else 0
    preds = np.full(len(y), majority)
    return f1_score(y, preds, zero_division=0)


def evaluate(sub, label):
    X = sub[FEATURE_COLS].values
    y = sub["label"].values
    groups = sub["episode_id"].values

    true, pred = loo_cv_eval(X, y, groups)
    precision = precision_score(true, pred, zero_division=0)
    recall = recall_score(true, pred, zero_division=0)
    f1 = f1_score(true, pred, zero_division=0)
    trivial_f1 = trivial_majority_f1(y)

    rng = np.random.default_rng(SEED)
    shuffled_f1s = []
    for _ in range(N_SHUFFLES):
        y_shuffled = rng.permutation(y)
        t, p = loo_cv_eval(X, y_shuffled, groups)
        shuffled_f1s.append(f1_score(t, p, zero_division=0))
    n_ge = sum(1 for f in shuffled_f1s if f >= f1)
    p_value = n_ge / N_SHUFFLES
    p_value_reported = f"< {1/N_SHUFFLES:.2g}" if n_ge == 0 else f"{p_value:.3f}"

    print(f"executor_oom ({label}): precision={precision:.3f} recall={recall:.3f} f1={f1:.3f}")
    print(f"  trivial always-majority-class F1: {trivial_f1:.3f}  (real f1 {'>' if f1 > trivial_f1 else '<='} trivial)")
    print(f"  shuffled F1: mean={np.mean(shuffled_f1s):.3f} range=[{np.min(shuffled_f1s):.3f},{np.max(shuffled_f1s):.3f}]")
    print(f"  {n_ge}/{N_SHUFFLES} shuffled F1 >= real F1  ->  p {p_value_reported}")

    return {
        "precision": precision, "recall": recall, "f1": f1,
        "trivial_majority_class_f1": trivial_f1,
        "beats_trivial_baseline": f1 > trivial_f1,
        "n_shuffles": N_SHUFFLES,
        "shuffled_f1_mean": float(np.mean(shuffled_f1s)),
        "shuffled_f1_std": float(np.std(shuffled_f1s)),
        "n_shuffled_ge_real": n_ge,
        "p_value": p_value,
        "p_value_reported": p_value_reported,
    }


def main():
    df = pd.read_csv(REPO / "results" / "ml-first-pass" / "extracted_windows.csv")
    full = df[df.fault_class == "executor_oom"].reset_index(drop=True)
    clean = full[full.episode_id.isin(CLEAN_EPISODES)].reset_index(drop=True)

    result = {
        "full_5_episode": evaluate(full, "5-episode"),
        "clean_3_episode": evaluate(clean, "3-clean-episode"),
    }
    with open(REPO / "results" / "ml-first-pass" / "executor_oom_null_baseline_check.json", "w") as f:
        json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()
