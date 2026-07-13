"""Closes the first accepted-limitation gap from the Weeks 8-9 gate-audit (Section 8):
RF/XGBoost/LightGBM trained and tuned, at whatever window/horizon config
results/ml-first-pass/extracted_windows.csv currently holds (this script does not
re-extract -- only the window/horizon SWEEP, in a separate script, does that). Originally
run at [15s,30s]/30s; re-run at [10s,15s]/15s after Section 6.3's second correction
(2026-07-13) found the former unsafe for every class's tightest real inter-episode gap
-- current committed results/JSON are from the corrected config.

Nested CV per Section 6.3's addendum: for each outer LOO fold, hyperparameters are
selected via an inner CV on the outer-training groups only, never touching the outer
test group. Inner CV uses group k-fold (k=5) rather than full inner-LOO -- full nested
LOO-of-LOO on 17 training groups would mean 17 inner fits x 3 configs x 18 outer folds
x 3 models = ~2750 fits just for one class; 5-fold cuts that ~3.4x with no loss of
principle (nested CV is standard with an inner k-fold even when the outer loop is LOO).

executor_oom is NOT nested-tuned. With only 4 (or 2, clean-3 subset) training groups
remaining per outer fold, an inner CV has no meaningful signal to select on -- this is
the same small-N problem the null-baseline check already confirmed (F1 below a trivial
baseline, chance-level p across configs tried). Faking a tuned result on data that can't
support tuning would
misrepresent the finding, not strengthen it. executor_oom still gets all 3 models
trained with one fixed default config each, satisfying the "trained" half of Section 8's
gate criterion honestly, without pretending the "tuned" half applies at this N.
"""
import json
import warnings
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
import lightgbm as lgb
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import LeaveOneGroupOut, GroupKFold
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import precision_score, recall_score, f1_score

warnings.filterwarnings("ignore", message="X does not have valid feature names")

REPO = Path(__file__).resolve().parent.parent
FEATURE_COLS = ["mean", "std", "min", "max", "last", "n_samples"]
CLEAN_EXEC_OOM_EPISODES = ["ramptest3", "ramptest7", "ramptest10"]
N_SHUFFLES = 100
SEED = 42

RF_GRID = [
    {"n_estimators": 100, "max_depth": 3},
    {"n_estimators": 200, "max_depth": 5},
    {"n_estimators": 300, "max_depth": 7},
]
XGB_GRID = [
    {"n_estimators": 100, "max_depth": 3, "learning_rate": 0.1},
    {"n_estimators": 200, "max_depth": 3, "learning_rate": 0.1},
    {"n_estimators": 100, "max_depth": 5, "learning_rate": 0.05},
]
LGB_GRID = [
    {"n_estimators": 100, "max_depth": 3, "learning_rate": 0.1, "min_child_samples": 1},
    {"n_estimators": 200, "max_depth": 3, "learning_rate": 0.1, "min_child_samples": 1},
    {"n_estimators": 100, "max_depth": 5, "learning_rate": 0.05, "min_child_samples": 1},
]
MODEL_GRIDS = {"random_forest": RF_GRID, "xgboost": XGB_GRID, "lightgbm": LGB_GRID}
EXEC_OOM_DEFAULT = {"random_forest": RF_GRID[1], "xgboost": XGB_GRID[0], "lightgbm": LGB_GRID[0]}


def build_model(name, params, y_train):
    # n_jobs=1 everywhere: on data this tiny (dozens of rows), sklearn/XGBoost/LightGBM's
    # default thread-pool spin-up per .fit() call costs more than the fit itself --
    # measured 7.5x speedup for XGBoost (0.071s/fit -> 0.0094s/fit) with no accuracy
    # difference (single-threaded is exact, not an approximation).
    if name == "random_forest":
        return RandomForestClassifier(random_state=SEED, class_weight="balanced", n_jobs=1, **params)
    if name == "xgboost":
        n_pos = int((y_train == 1).sum())
        n_neg = int((y_train == 0).sum())
        scale_pos_weight = (n_neg / n_pos) if n_pos > 0 else 1.0
        return xgb.XGBClassifier(random_state=SEED, scale_pos_weight=scale_pos_weight,
                                  eval_metric="logloss", n_jobs=1, **params)
    if name == "lightgbm":
        return lgb.LGBMClassifier(random_state=SEED, class_weight="balanced", verbosity=-1, n_jobs=1, **params)
    raise ValueError(name)


def fit_predict(name, params, X_train, y_train, X_test):
    if len(set(y_train)) < 2:
        return np.full(len(X_test), y_train[0])
    clf = build_model(name, params, y_train)
    clf.fit(X_train, y_train)
    return clf.predict(X_test)


def inner_tune(name, X_train, y_train, groups_train):
    grid = MODEL_GRIDS[name]
    n_groups = len(set(groups_train))
    if n_groups < 3:
        return None, "skipped: fewer than 3 training groups, no meaningful inner CV"
    n_splits = min(5, n_groups)
    gkf = GroupKFold(n_splits=n_splits)
    best_params, best_score = None, -1.0
    scores_by_config = []
    for params in grid:
        inner_true, inner_pred = [], []
        for tr_idx, te_idx in gkf.split(X_train, y_train, groups_train):
            if len(set(y_train[tr_idx])) < 2:
                continue
            scaler = StandardScaler()
            Xtr = scaler.fit_transform(X_train[tr_idx])
            Xte = scaler.transform(X_train[te_idx])
            preds = fit_predict(name, params, Xtr, y_train[tr_idx], Xte)
            inner_true.extend(y_train[te_idx].tolist())
            inner_pred.extend(preds.tolist())
        score = f1_score(inner_true, inner_pred, zero_division=0) if inner_true else 0.0
        scores_by_config.append({"params": params, "inner_f1": score})
        if score > best_score:
            best_score, best_params = score, params
    return best_params, scores_by_config


def loo_cv_nested(name, X, y, groups, tune, fixed_config=None):
    """tune=True: inner-CV hyperparameter selection per outer fold (used once, on the
    real labels, to find what nested-CV actually settles on). tune=False: every fold
    uses `fixed_config` (or the executor_oom default if none given) -- used for the
    shuffle test, which must hold the pipeline fixed and permute only the labels. Re-
    running full nested tuning inside every one of 100 shuffles was tried first and
    costs ~29,000 fits/class (100x the tuning cost) for no methodological benefit --
    the permutation null is "does this fixed, already-chosen model beat chance," not
    "does re-tuning-from-scratch on scrambled labels beat chance."
    """
    logo = LeaveOneGroupOut()
    all_true, all_pred, chosen_configs = [], [], []
    for train_idx, test_idx in logo.split(X, y, groups):
        y_train_raw = y[train_idx]
        groups_train = groups[train_idx]
        if tune:
            best_params, detail = inner_tune(name, X[train_idx], y_train_raw, groups_train)
            if best_params is None:
                best_params = EXEC_OOM_DEFAULT[name]
        else:
            best_params = fixed_config if fixed_config is not None else EXEC_OOM_DEFAULT[name]
        chosen_configs.append(best_params)

        scaler = StandardScaler()
        X_train = scaler.fit_transform(X[train_idx])
        X_test = scaler.transform(X[test_idx])
        preds = fit_predict(name, best_params, X_train, y_train_raw, X_test)
        all_true.extend(y[test_idx].tolist())
        all_pred.extend(preds.tolist())
    return all_true, all_pred, chosen_configs


def mean_feature_importance(name, params, X, y, groups):
    """Averaged across LOO folds at one fixed config -- same diagnostic that caught
    broker_kill's n_samples leak earlier this session, now run automatically for
    whichever model wins each class here, since XGBoost/LightGBM's own hyperparameter
    grids can exploit a feature-level leak that a different model's grid does not (see
    network_degradation below: RF/LightGBM landed near the honest ablation-confirmed
    number, XGBoost's grid found and exploited the same n_samples grid-misalignment
    artifact broker_kill already exposed, hitting F1=0.941/p<0.01 on a class already
    confirmed negative).
    """
    logo = LeaveOneGroupOut()
    importances = []
    for train_idx, _ in logo.split(X, y, groups):
        y_train = y[train_idx]
        if len(set(y_train)) < 2:
            continue
        scaler = StandardScaler()
        X_train = scaler.fit_transform(X[train_idx])
        clf = build_model(name, params, y_train)
        clf.fit(X_train, y_train)
        importances.append(clf.feature_importances_)
    return np.mean(importances, axis=0) if importances else np.zeros(len(FEATURE_COLS))


def shuffle_test(name, X, y, groups, fixed_config, real_f1):
    rng = np.random.default_rng(SEED)
    shuffled_f1s = []
    for _ in range(N_SHUFFLES):
        y_shuffled = rng.permutation(y)
        t, p, _ = loo_cv_nested(name, X, y_shuffled, groups, tune=False, fixed_config=fixed_config)
        shuffled_f1s.append(f1_score(t, p, zero_division=0))
    n_ge = sum(1 for f in shuffled_f1s if f >= real_f1)
    p_value = n_ge / N_SHUFFLES
    p_value_reported = f"< {1/N_SHUFFLES:.2g}" if n_ge == 0 else f"{p_value:.3f}"
    return {"n_shuffled_ge_real": n_ge, "p_value": p_value, "p_value_reported": p_value_reported,
            "shuffled_f1_mean": float(np.mean(shuffled_f1s)), "shuffled_f1_std": float(np.std(shuffled_f1s)),
            "fixed_config_used": fixed_config}


def evaluate_class(cls, sub, tune):
    X = sub[FEATURE_COLS].values
    y = sub["label"].values
    groups = sub["episode_id"].values
    results = {}
    best_model, best_f1, best_fixed_config = None, -1.0, None
    for name in MODEL_GRIDS:
        true, pred, chosen_configs = loo_cv_nested(name, X, y, groups, tune)
        precision = precision_score(true, pred, zero_division=0)
        recall = recall_score(true, pred, zero_division=0)
        f1 = f1_score(true, pred, zero_division=0)
        results[name] = {
            "tuned": tune, "precision": precision, "recall": recall, "f1": f1,
            "n_windows": len(sub), "n_groups": len(set(groups)),
        }
        # mode config: the single hyperparameter setting nested-CV chose most often
        # across outer folds -- used as the ONE fixed pipeline for this model's
        # shuffle test (see loo_cv_nested's docstring for why tuning isn't re-run
        # inside every permutation).
        config_counts = Counter(json.dumps(c, sort_keys=True) for c in chosen_configs)
        mode_config = json.loads(config_counts.most_common(1)[0][0])
        if tune:
            results[name]["chosen_config_frequency"] = dict(config_counts)
            results[name]["mode_config"] = mode_config
        else:
            results[name]["fixed_config"] = EXEC_OOM_DEFAULT[name]
        print(f"  {cls}/{name}: precision={precision:.3f} recall={recall:.3f} f1={f1:.3f}", flush=True)
        if f1 > best_f1:
            best_f1, best_model, best_fixed_config = f1, name, mode_config

    imp = mean_feature_importance(best_model, best_fixed_config, X, y, groups)
    imp_ranked = sorted(zip(FEATURE_COLS, imp.tolist()), key=lambda x: -x[1])
    n_samples_importance = dict(imp_ranked)["n_samples"]
    leak_flagged = n_samples_importance > 0.5
    results["best_model_feature_importance"] = imp_ranked
    results["best_model_n_samples_leak_flagged"] = leak_flagged
    print(f"  {cls}/{best_model} feature importance: {imp_ranked} "
          f"({'LEAK FLAGGED, n_samples>0.5' if leak_flagged else 'no single-feature dominance'})", flush=True)

    eval_name, eval_X, eval_fixed_config = best_model, X, best_fixed_config
    if leak_flagged:
        no_ns_cols = [c for c in FEATURE_COLS if c != "n_samples"]
        X_no_ns = sub[no_ns_cols].values
        true_ns, pred_ns, _ = loo_cv_nested(best_model, X_no_ns, y, groups, tune=False, fixed_config=best_fixed_config)
        deconf = {
            "precision": precision_score(true_ns, pred_ns, zero_division=0),
            "recall": recall_score(true_ns, pred_ns, zero_division=0),
            "f1": f1_score(true_ns, pred_ns, zero_division=0),
            "features_used": no_ns_cols,
        }
        results["best_model_deconfounded_no_n_samples"] = deconf
        print(f"  {cls}/{best_model} de-confounded (no n_samples): precision={deconf['precision']:.3f} "
              f"recall={deconf['recall']:.3f} f1={deconf['f1']:.3f} -- using THIS for the significance "
              f"test, not the leaked full-feature number", flush=True)
        eval_X, eval_fixed_config = X_no_ns, best_fixed_config
        best_f1 = deconf["f1"]

    print(f"  -> best model for {cls}: {best_model} (f1={best_f1:.3f}{', de-confounded' if leak_flagged else ''}), "
          f"running {N_SHUFFLES}-shuffle significance test with its mode config fixed: {eval_fixed_config}", flush=True)
    sig = shuffle_test(eval_name, eval_X, y, groups, eval_fixed_config, best_f1)
    results["best_model"] = best_model
    results["best_model_significance"] = sig
    results["best_model_significance_basis"] = "de-confounded (no n_samples)" if leak_flagged else "raw feature set"
    print(f"  {cls}/{best_model} significance ({results['best_model_significance_basis']}): "
          f"{sig['n_shuffled_ge_real']}/{N_SHUFFLES} >= real -> p {sig['p_value_reported']}", flush=True)
    return results


def main():
    df = pd.read_csv(REPO / "results" / "ml-first-pass" / "extracted_windows.csv")
    all_results = {}
    for cls in ["broker_kill", "disk_pressure", "network_degradation"]:
        print(f"=== {cls} (nested-tuned) ===")
        sub = df[df.fault_class == cls].reset_index(drop=True)
        all_results[cls] = evaluate_class(cls, sub, tune=True)

    print("=== executor_oom (untuned, N too small for nested CV) ===")
    full = df[df.fault_class == "executor_oom"].reset_index(drop=True)
    clean = full[full.episode_id.isin(CLEAN_EXEC_OOM_EPISODES)].reset_index(drop=True)
    all_results["executor_oom_5episode"] = evaluate_class("executor_oom_5episode", full, tune=False)
    all_results["executor_oom_clean3"] = evaluate_class("executor_oom_clean3", clean, tune=False)

    with open(REPO / "results" / "ml-first-pass" / "multi_model_nested_tuning.json", "w") as f:
        json.dump(all_results, f, indent=2)


if __name__ == "__main__":
    main()
