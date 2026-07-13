"""Delta/relative-baseline features for disk_pressure, built per the diagnostic finding:
false positives in the std-only ablation were scattered across the full 67-minute quiet
period (not clustered by time-of-day), so a per-episode delta baseline (not a
relative-to-recent-local-baseline scheme) is the appropriate fix for the absolute-value
drift confound found in the full-feature-set result.

Baseline per real episode: disk_pressure's own baseline_avail_bytes, measured by the
fault script right before injection (real, not re-derived). Baseline for the synthetic
normal_reference windows: the quiet period's own mean (994899156269.18, computed from
all 68 real samples across 2026-07-11T17:13:00Z-18:20:00Z) -- they don't belong to a real
episode, so their "local baseline" is the reference period's own central tendency.

Re-runs LOO-CV (full delta feature set) + 100-shuffle permutation test, same rigor as
every other check this session.
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

EPISODE_BASELINES = {
    "campaign1": 995152609280.0, "campaign2": 995152027648.0, "campaign3": 995150802944.0,
    "campaign4": 995149934592.0, "campaign5": 995148972032.0, "campaign7": 995146944512.0,
    "campaign8": 995145748480.0, "topup1": 995050299392.0,
}
QUIET_PERIOD_MEAN = 994899156269.1765

DELTA_FEATURE_COLS = ["delta_mean", "delta_min", "delta_max", "delta_last", "std", "n_samples"]
N_SHUFFLES = 100
SEED = 42


def baseline_for(episode_id):
    return EPISODE_BASELINES.get(episode_id, QUIET_PERIOD_MEAN)


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
    sub = df[df.fault_class == "disk_pressure"].reset_index(drop=True)
    y = sub["label"].values
    groups = sub["episode_id"].values

    # Comparison numbers computed from the CURRENT data, not frozen literals -- a
    # hardcoded "what the other approaches scored" goes stale the moment the window/
    # horizon config changes (found happening to exactly this pattern in 3 other
    # scripts during the 2026-07-13 recalibration; fixed here too instead of hand-
    # patching this script's JSON output the same way those were originally fixed).
    raw_cols = ["mean", "std", "min", "max", "last", "n_samples"]
    raw_true, raw_pred = loo_cv_eval(sub[raw_cols].values, y, groups)
    comparison_full_raw_features_f1 = f1_score(raw_true, raw_pred, zero_division=0)

    std_true, std_pred = loo_cv_eval(sub[["std"]].values, y, groups)
    comparison_std_only_f1 = f1_score(std_true, std_pred, zero_division=0)
    rng_std = np.random.default_rng(SEED)
    std_shuffled_f1s = []
    for _ in range(N_SHUFFLES):
        y_shuffled = rng_std.permutation(y)
        t, p = loo_cv_eval(sub[["std"]].values, y_shuffled, groups)
        std_shuffled_f1s.append(f1_score(t, p, zero_division=0))
    comparison_std_only_p = sum(1 for f in std_shuffled_f1s if f >= comparison_std_only_f1) / N_SHUFFLES

    baselines = sub["episode_id"].map(baseline_for)
    sub["delta_mean"] = sub["mean"] - baselines
    sub["delta_min"] = sub["min"] - baselines
    sub["delta_max"] = sub["max"] - baselines
    sub["delta_last"] = sub["last"] - baselines

    X = sub[DELTA_FEATURE_COLS].values

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
    # A rank-based p-value from N_SHUFFLES permutations is bounded below by 1/N_SHUFFLES --
    # 0/N_SHUFFLES means "p < 1/N_SHUFFLES", not "p is exactly 0". Report the bound, not
    # the raw fraction, when n_ge == 0.
    p_value_reported = f"< {1/N_SHUFFLES:.2g}" if n_ge == 0 else f"{p_value:.3f}"

    print(f"disk_pressure delta features: precision={precision:.3f} recall={recall:.3f} f1={f1:.3f}")
    print(f"shuffled F1: mean={np.mean(shuffled_f1s):.3f} range=[{np.min(shuffled_f1s):.3f},{np.max(shuffled_f1s):.3f}]")
    print(f"{n_ge}/{N_SHUFFLES} shuffled F1 >= real F1  ->  p {p_value_reported}")

    result = {
        "features_used": DELTA_FEATURE_COLS,
        "precision": precision, "recall": recall, "f1": f1,
        "n_shuffles": N_SHUFFLES,
        "shuffled_f1_mean": float(np.mean(shuffled_f1s)),
        "shuffled_f1_std": float(np.std(shuffled_f1s)),
        "n_shuffled_ge_real": n_ge, "p_value": p_value,
        "p_value_reported": p_value_reported,
        "p_value_note": "rank-based p-value from N_SHUFFLES permutations is bounded below by 1/N_SHUFFLES; 0/N_SHUFFLES is reported as a bound, not an exact 0.",
        "comparison_full_raw_features_f1": comparison_full_raw_features_f1,
        "comparison_std_only_f1": comparison_std_only_f1,
        "comparison_std_only_p": comparison_std_only_p,
    }
    with open(REPO / "results" / "ml-first-pass" / "disk_pressure_delta_features.json", "w") as f:
        json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()
