"""Weeks 8-9 first model training pass: window extraction + Random Forest, LOO-CV.

Reads episode ground truth directly from results/campaign-n8/<class>/*.json (not
hardcoded timestamps) for the 4 classes with a viable static-threshold metric per
docs/baseline_thresholds.md. backpressure_cascade stays excluded (no independent
telemetry signal, same finding as the baseline).

Per docs/research_context.md Section 6.3 addendum:
  1. Split unit is the episode -- every window from a given rep gets the same group id.
  2. Leave-one-episode-out CV, not a fixed ratio split.
  3. Applied to all 4 classes including broker_kill, per agreed treatment.
  4. Scaler fit exclusively on each fold's training episodes, refit per fold.
"""
import json
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import LeaveOneGroupOut
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import precision_score, recall_score, f1_score

REPO = Path(__file__).resolve().parent.parent
CAMPAIGN_DIR = REPO / "results" / "campaign-n8"

# Horizons/window recalibrated against REAL inter-episode spacing, not an a-priori
# textbook choice -- per Section 6.3, "window size and horizon are hyperparameters to
# sweep, not fixed a priori." First attempt used [30,60,120]s horizons + 90s window
# (needing ~210s of lookback room), which collapsed almost every episode's "normal"
# window (7 of 8 skipped for most classes) because real inter-episode gaps in this
# campaign are only 32-90s (measured directly from the ground-truth JSONs, not
# assumed). Shrunk to fit what the actual campaign spacing supports.
HORIZONS_S = [15, 30]
WINDOW_S = 30

# Quiet reference period for broker_kill/disk_pressure/network_degradation's "normal"
# windows -- these three share the same broker-side telemetry surface. Reusing the
# already-validated fault-free window from this session's disk_pressure noise-floor
# check (results/baseline-threshold-evidence/prometheus_derived_figures.json), rather
# than trying to carve "normal" out of the same tight 32-90s inter-episode gaps used
# for pre-failure horizons above -- there isn't room for both in the same gap.
QUIET_REF_START = "2026-07-11T17:13:00Z"
QUIET_REF_END = "2026-07-11T18:20:00Z"
N_NORMAL_REF_WINDOWS = 10
NORMAL_REF_WINDOW_S = WINDOW_S  # MUST match pre-failure's window size -- a first attempt
# used 300s here against pre-failure's 30s, which made n_samples (~21 vs ~3) a perfect,
# completely spurious label leak (window-size artifact, not signal). Caught by inspecting
# the raw extracted features before trusting a suspicious 1.000 F1, not assumed fine.
#
# SECOND BUG, caught later (disk_pressure false-positive clustering investigation):
# that fix shrank the window size but left the stride between windows at exactly
# NORMAL_REF_WINDOW_S too, packing all 10 windows into the first 300s (5 minutes) of
# the intended 67-minute quiet period, not spread across it -- and that 5-minute slice
# (17:13:00-17:18:00) sits entirely inside executor_oom's own ramptest3 fault window
# (injected 17:13:37, OOM 17:17:06). Every "normal" sample was drawn from inside a
# DIFFERENT class's active fault, not a genuinely separate quiet time. Fixed by
# striding windows evenly across the full QUIET_REF_START..QUIET_REF_END span instead.
NORMAL_REF_STRIDE_S = 402  # (4020s span) / 10 windows, spreads evenly end to end

# executor_oom is pod-scoped (each episode's target_pod is a fresh, short-lived pod --
# no shared long-lived "normal" pod exists), so its normal window stays tied to each
# episode's own pre-injection settling period instead, positioned deep enough to avoid
# the pre-failure horizon zone above (which reaches back to onset-60s).
EXEC_OOM_NORMAL_END_OFFSET_S = -90
EXEC_OOM_NORMAL_WINDOW_S = 60

CLASS_CONFIG = {
    "broker_kill": {
        "dir": "broker_kill",
        "metric": 'up{{job="kafka-broker-jmx"}}',
        "onset_field": "injection_timestamp_utc",
        "end_field": "target_recovered_utc",
        "pod_scoped": False,
    },
    "executor_oom": {
        "dir": "executor_oom",
        "metric": 'container_memory_working_set_bytes{{pod="{pod}", container="spark-kubernetes-executor"}}',
        "onset_field": "injection_timestamp_utc",
        "end_field": "oomkilled_confirmed_utc",
        "pod_scoped": True,
    },
    "disk_pressure": {
        "dir": "disk_pressure",
        "metric": 'node_filesystem_avail_bytes{{mountpoint="/var"}}',
        "onset_field": "injection_timestamp_utc",
        "end_field": "cleaned_up_utc",
        "pod_scoped": False,
    },
    "network_degradation": {
        "dir": "network_degradation",
        "metric": 'scrape_duration_seconds{{job="kafka-broker-jmx"}}',
        "onset_field": "injection_timestamp_utc",
        "end_field": "target_recovered_utc",
        "pod_scoped": False,
    },
}


def iso(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def add(dt, seconds):
    return dt + timedelta(seconds=seconds)


def load_episodes(cls):
    cfg = CLASS_CONFIG[cls]
    files = sorted((CAMPAIGN_DIR / cfg["dir"]).glob("*.json"))
    episodes = []
    for f in files:
        d = json.loads(f.read_text())
        episodes.append(d)
    episodes.sort(key=lambda d: d[cfg["onset_field"]])
    return episodes


class PromClient:
    def __init__(self):
        self.proc = subprocess.Popen(
            ["kubectl", "-n", "monitoring", "port-forward", "svc/prometheus-server", "19192:80"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        time.sleep(3)
        self.base = "http://localhost:19192/api/v1/query_range"

    def query_range(self, query, start_dt, end_dt, step="15s"):
        params = urllib.parse.urlencode({
            "query": query,
            "start": start_dt.isoformat(),
            "end": end_dt.isoformat(),
            "step": step,
        })
        req = urllib.request.urlopen(f"{self.base}?{params}", timeout=20)
        d = json.loads(req.read())
        series = d.get("data", {}).get("result", [])
        out = []
        for s in series:
            for t, v in s["values"]:
                out.append((int(t), float(v)))
        out.sort()
        return out

    def close(self):
        self.proc.terminate()


def window_features(samples, win_start, win_end):
    vals = [v for t, v in samples if win_start.timestamp() <= t <= win_end.timestamp()]
    if not vals:
        return None
    arr = np.array(vals, dtype=float)
    return {
        "mean": float(arr.mean()),
        "std": float(arr.std()) if len(arr) > 1 else 0.0,
        "min": float(arr.min()),
        "max": float(arr.max()),
        "last": float(arr[-1]),
        "n_samples": len(arr),
    }


def extract_pre_failure_windows(cls, prom):
    cfg = CLASS_CONFIG[cls]
    episodes = load_episodes(cls)
    rows = []
    skipped = []
    for ep in episodes:
        onset = iso(ep[cfg["onset_field"]])
        end = iso(ep[cfg["end_field"]]) if ep.get(cfg["end_field"]) else onset
        query = cfg["metric"].format(pod=ep.get("target_pod", ""))
        episode_id = ep["run_id"]

        query_start = add(onset, -max(HORIZONS_S) - WINDOW_S - 30)
        query_end = add(end, 30)
        samples = prom.query_range(query, query_start, query_end, step="15s")

        for h in HORIZONS_S:
            win_end = add(onset, -h)
            win_start = add(win_end, -WINDOW_S)
            feats = window_features(samples, win_start, win_end)
            if feats is None:
                skipped.append((episode_id, h))
                continue
            rows.append({
                "fault_class": cls, "episode_id": episode_id, "label": 1,
                "horizon_s": h, "window_kind": "pre_failure", **feats,
            })
    if skipped:
        print(f"  [{cls}] pre-failure window skipped (no real samples) for: {skipped}")
    return rows


def extract_normal_reference_windows(cls, prom):
    """broker_kill/disk_pressure/network_degradation: independent normal windows drawn
    from a genuinely quiet, already-validated period, not squeezed out of the same tight
    inter-episode gaps used for pre-failure horizons. Each window gets its own synthetic
    group id -- they are mutually independent (non-overlapping, no shared fault episode),
    so grouping them separately from the real fault episodes doesn't violate the no-leakage
    rule; it just means they participate in LOO-CV as their own distinct "episodes."
    """
    cfg = CLASS_CONFIG[cls]
    query = cfg["metric"].format(pod="")
    ref_start = iso(QUIET_REF_START)
    samples = prom.query_range(query, ref_start, iso(QUIET_REF_END), step="15s")

    rows = []
    for i in range(N_NORMAL_REF_WINDOWS):
        win_start = add(ref_start, i * NORMAL_REF_STRIDE_S)
        win_end = add(win_start, NORMAL_REF_WINDOW_S)
        feats = window_features(samples, win_start, win_end)
        if feats is None:
            continue
        rows.append({
            "fault_class": cls, "episode_id": f"normal_ref_{i+1}", "label": 0,
            "horizon_s": None, "window_kind": "normal_reference", **feats,
        })
    return rows


def extract_executor_oom_normal_windows(prom):
    cfg = CLASS_CONFIG["executor_oom"]
    episodes = load_episodes("executor_oom")
    rows = []
    skipped = []
    for ep in episodes:
        onset = iso(ep[cfg["onset_field"]])
        query = cfg["metric"].format(pod=ep["target_pod"])
        episode_id = ep["run_id"]
        win_end = add(onset, EXEC_OOM_NORMAL_END_OFFSET_S)
        win_start = add(win_end, -EXEC_OOM_NORMAL_WINDOW_S)
        samples = prom.query_range(query, win_start, win_end, step="15s")
        feats = window_features(samples, win_start, win_end)
        if feats is None:
            skipped.append(episode_id)
            continue
        rows.append({
            "fault_class": "executor_oom", "episode_id": episode_id, "label": 0,
            "horizon_s": None, "window_kind": "normal", **feats,
        })
    if skipped:
        print(f"  [executor_oom] normal window skipped (cold-start pod, no settled "
              f"reading yet) for: {skipped}")
    return rows


def extract_class(cls, prom):
    rows = extract_pre_failure_windows(cls, prom)
    if cls == "executor_oom":
        rows += extract_executor_oom_normal_windows(prom)
    else:
        rows += extract_normal_reference_windows(cls, prom)
    return pd.DataFrame(rows)


def loo_cv_train_eval(df, cls):
    feature_cols = ["mean", "std", "min", "max", "last", "n_samples"]
    X = df[feature_cols].values
    y = df["label"].values
    groups = df["episode_id"].values

    logo = LeaveOneGroupOut()
    all_true, all_pred = [], []
    per_fold = []
    for train_idx, test_idx in logo.split(X, y, groups):
        scaler = StandardScaler()
        X_train = scaler.fit_transform(X[train_idx])
        X_test = scaler.transform(X[test_idx])
        y_train = y[train_idx]

        held_out_episode = groups[test_idx][0]
        if len(set(y_train)) < 2:
            # training fold has only one class present -- can't fit a classifier meaningfully;
            # predict the majority class trivially and record it, don't silently skip the fold
            majority = y_train[0]
            preds = np.full(len(test_idx), majority)
        else:
            clf = RandomForestClassifier(n_estimators=200, max_depth=5, random_state=42, class_weight="balanced")
            clf.fit(X_train, y_train)
            preds = clf.predict(X_test)

        all_true.extend(y[test_idx].tolist())
        all_pred.extend(preds.tolist())
        per_fold.append({
            "held_out_episode": held_out_episode,
            "n_test_windows": len(test_idx),
            "true": y[test_idx].tolist(),
            "pred": preds.tolist(),
            "train_class_balance": {"normal": int((y_train == 0).sum()), "pre_failure": int((y_train == 1).sum())},
        })

    precision = precision_score(all_true, all_pred, zero_division=0)
    recall = recall_score(all_true, all_pred, zero_division=0)
    f1 = f1_score(all_true, all_pred, zero_division=0)
    return {
        "fault_class": cls,
        "n_windows_total": len(df),
        "n_episodes": df["episode_id"].nunique(),
        "class_balance": {"normal": int((y == 0).sum()), "pre_failure": int((y == 1).sum())},
        "precision": precision, "recall": recall, "f1": f1,
        "per_fold": per_fold,
    }


def main():
    prom = PromClient()
    results = {}
    all_windows = []
    try:
        for cls in CLASS_CONFIG:
            print(f"=== {cls}: extracting windows ===")
            df = extract_class(cls, prom)
            all_windows.append(df)
            print(f"  {len(df)} windows across {df['episode_id'].nunique()} episodes "
                  f"(pre_failure={int((df['label']==1).sum())}, normal={int((df['label']==0).sum())})")
            res = loo_cv_train_eval(df, cls)
            results[cls] = res
            print(f"  LOO-CV: precision={res['precision']:.3f} recall={res['recall']:.3f} f1={res['f1']:.3f}")
    finally:
        prom.close()

    full_df = pd.concat(all_windows, ignore_index=True)
    out_dir = REPO / "results" / "ml-first-pass"
    out_dir.mkdir(parents=True, exist_ok=True)
    full_df.to_csv(out_dir / "extracted_windows.csv", index=False)
    with open(out_dir / "loo_cv_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)

    print()
    print("=== summary ===")
    for cls, res in results.items():
        print(f"{cls}: n_windows={res['n_windows_total']} n_episodes={res['n_episodes']} "
              f"class_balance={res['class_balance']} "
              f"precision={res['precision']:.3f} recall={res['recall']:.3f} f1={res['f1']:.3f}")


if __name__ == "__main__":
    main()
