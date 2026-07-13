"""Weeks 10-11 follow-up (2026-07-13): executor_oom topped up from N=8 to N=15 real
episodes (fault_injection/campaign.py --top-up-class executor_oom --top-up-target 15,
same executor_oom.py gradual-ramp script, no config change -- see the topup manifest at
results/campaign-n8/topup_executor_oom_manifest_*.json for the raw collection record).

Re-runs the full evaluation pipeline at the new N: window extraction (unmodified
extract_pre_failure_windows/extract_executor_oom_normal_windows from
modeling/extract_and_train.py -- same code, not a reimplementation), LOO-CV,
majority-class trivial baseline, rank-based permutation/shuffle test (100 shuffles,
seed=42 -- identical protocol to executor_oom_null_baseline_check.py and every other
class), and feature-importance leak check. Only executor_oom's rows in
extracted_windows.csv and loo_cv_results.json are touched -- the other 3 classes'
already-gate-audited results are read but never rewritten.

Clean-episode classification (settled pre-onset baseline vs cold-start artifact) is
data-driven at this N, not a copy-pasted list: the N=8 pass hand-identified ramptest4/
ramptest8 as artifacts because their only normal-window sample was ~2.4MB against a
known real settled JVM+Spark footprint of ~484MB (see fault_injection/executor_oom.py's
ramp-redesign comment). Generalized here as an explicit threshold: a normal window whose
mean container_memory_working_set_bytes sits below 50MB (an order of magnitude below the
real ~484MB settled footprint, safely above 0 and nowhere near either real value, so no
episode is ambiguous under it) is a cold-start artifact, not a genuinely settled reading.
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import LeaveOneGroupOut
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import precision_score, recall_score, f1_score

sys.path.insert(0, str(Path(__file__).resolve().parent))
import extract_and_train as eat

REPO = Path(__file__).resolve().parent.parent
FEATURE_COLS = ["mean", "std", "min", "max", "last", "n_samples"]
COLD_START_THRESHOLD_BYTES = 50 * 1024 * 1024  # 50MB -- see module docstring
N_SHUFFLES = 100
SEED = 42


def loo_cv_eval(sub, seed=None, permute=False, rng=None):
    X = sub[FEATURE_COLS].values
    y = sub["label"].values
    if permute:
        y = rng.permutation(y)
    groups = sub["episode_id"].values
    logo = LeaveOneGroupOut()
    all_true, all_pred, per_fold = [], [], []
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
            "held_out_episode": held_out, "n_test_windows": len(test_idx),
            "true": y[test_idx].tolist(), "pred": preds.tolist(),
            "train_class_balance": {"normal": int((y_train == 0).sum()), "pre_failure": int((y_train == 1).sum())},
        })
    return all_true, all_pred, per_fold


def trivial_majority_f1(y):
    majority = 1 if (y == 1).sum() >= (y == 0).sum() else 0
    return f1_score(y, np.full(len(y), majority), zero_division=0)


def feature_importance(sub):
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
    avg = np.mean(importances, axis=0)
    return sorted(zip(FEATURE_COLS, avg.tolist()), key=lambda x: -x[1]), len(importances)


def evaluate(sub, label):
    y = sub["label"].values
    true, pred, per_fold = loo_cv_eval(sub)
    precision = precision_score(true, pred, zero_division=0)
    recall = recall_score(true, pred, zero_division=0)
    f1 = f1_score(true, pred, zero_division=0)
    trivial_f1 = trivial_majority_f1(y)

    rng = np.random.default_rng(SEED)
    shuffled_f1s = []
    for _ in range(N_SHUFFLES):
        t, p, _ = loo_cv_eval(sub, permute=True, rng=rng)
        shuffled_f1s.append(f1_score(t, p, zero_division=0))
    n_ge = sum(1 for f in shuffled_f1s if f >= f1)
    p_value = n_ge / N_SHUFFLES
    p_value_reported = f"< {1/N_SHUFFLES:.2g}" if n_ge == 0 else f"{p_value:.3f}"

    ranked, n_folds = feature_importance(sub)

    print(f"executor_oom ({label}): n_episodes={sub['episode_id'].nunique()} n_windows={len(sub)} "
          f"class_balance={{'normal': {int((y==0).sum())}, 'pre_failure': {int((y==1).sum())}}}")
    print(f"  precision={precision:.3f} recall={recall:.3f} f1={f1:.3f}  trivial_majority_f1={trivial_f1:.3f} "
          f"({'beats' if f1 > trivial_f1 else 'does NOT beat'} trivial)")
    print(f"  shuffled F1: mean={np.mean(shuffled_f1s):.3f} range=[{np.min(shuffled_f1s):.3f},{np.max(shuffled_f1s):.3f}] "
          f"-- {n_ge}/{N_SHUFFLES} shuffled>=real -> p {p_value_reported}")
    print(f"  feature importance: {[(f, round(i,3)) for f, i in ranked]}")

    return {
        "n_episodes": int(sub["episode_id"].nunique()), "n_windows": len(sub),
        "class_balance": {"normal": int((y == 0).sum()), "pre_failure": int((y == 1).sum())},
        "precision": precision, "recall": recall, "f1": f1,
        "trivial_majority_class_f1": trivial_f1, "beats_trivial_baseline": f1 > trivial_f1,
        "n_shuffles": N_SHUFFLES, "shuffled_f1_mean": float(np.mean(shuffled_f1s)),
        "shuffled_f1_std": float(np.std(shuffled_f1s)), "n_shuffled_ge_real": n_ge,
        "p_value": p_value, "p_value_reported": p_value_reported,
        "feature_importance_ranked": ranked, "n_importance_folds": n_folds,
        "per_fold": per_fold,
    }


def main():
    print("=== Step 1: re-extract executor_oom windows at N=15 (unmodified extraction code) ===")
    episodes = eat.load_episodes("executor_oom")
    print(f"  {len(episodes)} raw ground-truth episodes found: {[e['run_id'] for e in episodes]}")

    prom = eat.PromClient()
    try:
        pre_df = eat.extract_pre_failure_windows("executor_oom", prom)
        normal_rows = eat.extract_executor_oom_normal_windows(prom)
    finally:
        prom.close()
    pre_df = pd.DataFrame(pre_df)
    normal_df = pd.DataFrame(normal_rows)
    new_exec_df = pd.concat([pre_df, normal_df], ignore_index=True)

    inventory = (new_exec_df.groupby("episode_id")
                 .agg(n_windows=("label", "size"), labels=("label", lambda s: sorted(s.tolist())))
                 .reset_index())
    print("\n  per-episode window inventory:")
    all_episode_ids = [e["run_id"] for e in episodes]
    got_windows = set(new_exec_df["episode_id"].unique())
    for eid in all_episode_ids:
        if eid in got_windows:
            row = inventory[inventory.episode_id == eid].iloc[0]
            print(f"    {eid:10s}: {row.n_windows} window(s), labels={row.labels}")
        else:
            print(f"    {eid:10s}: 0 windows -- no real samples in ANY horizon (scrape-timing miss)")
    n_zero = len(all_episode_ids) - len(got_windows)
    print(f"  -> {len(got_windows)}/{len(all_episode_ids)} raw episodes yielded at least one usable window "
          f"({n_zero} lost entirely to scrape-timing misses)")

    print("\n=== Step 2: classify clean vs cold-start-artifact normal windows "
          f"(threshold: mean < {COLD_START_THRESHOLD_BYTES/1e6:.0f}MB) ===")
    normal_only = new_exec_df[new_exec_df.window_kind == "normal"]
    clean_episodes, artifact_episodes = [], []
    for _, row in normal_only.iterrows():
        tag = "COLD-START ARTIFACT" if row["mean"] < COLD_START_THRESHOLD_BYTES else "clean/settled"
        print(f"    {row.episode_id:10s}: normal-window mean={row['mean']/1e6:.1f}MB -> {tag}")
        (artifact_episodes if row["mean"] < COLD_START_THRESHOLD_BYTES else clean_episodes).append(row.episode_id)
    episodes_with_normal = set(normal_only.episode_id)
    episodes_without_normal = got_windows - episodes_with_normal
    for eid in sorted(episodes_without_normal):
        print(f"    {eid:10s}: no normal window extracted at all (skipped upstream) -> excluded from clean set")
    print(f"  clean (settled) episodes: {sorted(clean_episodes)}")
    print(f"  cold-start-artifact episodes (normal window present but excluded from clean set): {sorted(artifact_episodes)}")

    print("\n=== Step 3: full-N and clean-subset LOO-CV + trivial baseline + shuffle test ===")
    full_result = evaluate(new_exec_df, f"full {len(got_windows)}-episode")
    clean_sub = new_exec_df[new_exec_df.episode_id.isin(clean_episodes)].reset_index(drop=True)
    clean_result = evaluate(clean_sub, f"{len(clean_episodes)}-clean-episode")

    out = {
        "n_raw_episodes_collected": len(all_episode_ids),
        "raw_episode_ids": all_episode_ids,
        "n_episodes_with_any_window": len(got_windows),
        "episodes_with_zero_windows": sorted(set(all_episode_ids) - got_windows),
        "cold_start_threshold_bytes": COLD_START_THRESHOLD_BYTES,
        "clean_episodes": sorted(clean_episodes),
        "cold_start_artifact_episodes": sorted(artifact_episodes),
        "episodes_without_normal_window": sorted(episodes_without_normal),
        "full_set": full_result,
        "clean_set": clean_result,
    }
    out_path = REPO / "results" / "ml-first-pass" / "executor_oom_n15_evaluation.json"
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\nwrote {out_path}")

    print("\n=== Step 4: update canonical extracted_windows.csv / loo_cv_results.json "
          "(executor_oom rows only -- other 3 classes untouched) ===")
    full_csv_path = REPO / "results" / "ml-first-pass" / "extracted_windows.csv"
    full_csv = pd.read_csv(full_csv_path)
    other_classes = full_csv[full_csv.fault_class != "executor_oom"]
    combined = pd.concat([other_classes, new_exec_df], ignore_index=True)
    combined.to_csv(full_csv_path, index=False)
    print(f"  {full_csv_path}: replaced executor_oom's {len(got_windows)}-episode/{len(new_exec_df)}-window block "
          f"({len(other_classes)} other-class rows untouched)")

    loo_path = REPO / "results" / "ml-first-pass" / "loo_cv_results.json"
    loo = json.load(open(loo_path))
    loo["executor_oom"] = {
        "fault_class": "executor_oom",
        "n_windows_total": len(new_exec_df),
        "n_episodes": len(got_windows),
        "class_balance": full_result["class_balance"],
        "precision": full_result["precision"], "recall": full_result["recall"], "f1": full_result["f1"],
        "per_fold": full_result["per_fold"],
    }
    with open(loo_path, "w") as f:
        json.dump(loo, f, indent=2, default=str)
    print(f"  {loo_path}: executor_oom entry replaced (broker_kill/disk_pressure/network_degradation untouched)")


if __name__ == "__main__":
    main()
