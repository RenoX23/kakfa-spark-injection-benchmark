"""Weeks 10-11 ML lead-time reconstruction for disk_pressure, per the methodology
defined in docs/research_context.md Section 6.5's addendum -- corrected per user review
before running: the backward-scan bound must be the nearest episode of ANY class, not
just disk_pressure's own previous episode. Checking only same-class neighbors missed
that backpressure_cascade's campaign8 ends just 46s before disk_pressure's campaign1
onset -- the exact class of bug already found once this session (executor_oom's
ramptest3 overlapping the old normal-reference windows).

Reuses the 8 real-episode LOO folds from disk_pressure_delta_features.py's setup
(fit on the other 7 real episodes + all 10 normal_reference windows, per fold), but
refactored here to keep the fitted classifier+scaler objects instead of just
predictions, since lead-time reconstruction needs to run each fold's model
repeatedly across many scan positions, not once.
"""
import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import LeaveOneGroupOut
from sklearn.preprocessing import StandardScaler

import extract_and_train as eat

REPO = Path(__file__).resolve().parent.parent
CAMPAIGN_DIR = REPO / "results" / "campaign-n8"

EPISODE_BASELINES = {
    "campaign1": 995152609280.0, "campaign2": 995152027648.0, "campaign3": 995150802944.0,
    "campaign4": 995149934592.0, "campaign5": 995148972032.0, "campaign7": 995146944512.0,
    "campaign8": 995145748480.0, "topup1": 995050299392.0,
}
DELTA_FEATURE_COLS = ["delta_mean", "delta_min", "delta_max", "delta_last", "std", "n_samples"]
RAW_FEATURE_COLS = ["mean", "std", "min", "max", "last", "n_samples"]

WINDOW_S = 15
STEP_S = 15
MAX_SCAN_S = 300  # practical ceiling -- see Section 6.5 addendum point 5

CLASS_END_FIELDS = {
    "broker_kill": "target_recovered_utc",
    "executor_oom": "oomkilled_confirmed_utc",
    "disk_pressure": "cleaned_up_utc",
    "network_degradation": "target_recovered_utc",
    "backpressure_cascade": "caught_up_utc",
}

DISK_PRESSURE_EPISODES = ["campaign1", "campaign2", "campaign3", "campaign4", "campaign5",
                           "campaign7", "campaign8", "topup1"]

# item 4: no engineered precursor exists for this fault (near-instantaneous ~10s fill,
# triggered exactly at injection -- confirmed in docs/baseline_thresholds.md Section 4).
# Trained horizons are only 10-15s before onset; total fault duration for this class is
# 72-121s. 120s before ONSET already exceeds the longest total fault duration ever
# observed for this class and is 8x the trained horizon -- any detection beyond this has
# no known physical mechanism and gets manually traced for cross-class contamination.
FLAG_HORIZON_S = 120


def iso(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def load_all_episodes():
    """Every episode with a real injection_timestamp_utc, active AND discarded --
    discarded reps still had a real fault physically injected (only excluded from the
    N=8 active dataset for reasons like an unconfirmed drop timestamp, not because
    nothing happened), so their telemetry is real and present in Prometheus regardless
    of dataset-inclusion status. Missing this is exactly how campaign7's scan picked up
    discarded disk_pressure campaign6's real fill/cleanup, byte-for-byte identical to
    campaign6's own baseline_avail_bytes/post_fill_avail_bytes -- confirmed by manual
    trace, not assumed.
    """
    all_eps = []
    for cls, end_field in CLASS_END_FIELDS.items():
        for f in sorted((CAMPAIGN_DIR / cls).glob("*.json")):
            j = json.loads(f.read_text())
            onset = iso(j["injection_timestamp_utc"])
            end = iso(j.get(end_field) or j["injection_timestamp_utc"])
            all_eps.append({"cls": "ACTIVE:" + cls, "run_id": j["run_id"], "onset": onset, "end": end})
    for f in sorted((CAMPAIGN_DIR / "_discarded").rglob("*.json")):
        try:
            d = json.loads(f.read_text())
        except Exception:
            continue
        onset_raw = d.get("injection_timestamp_utc")
        if not onset_raw:
            continue
        onset = iso(onset_raw)
        end_raw = (d.get("target_recovered_utc") or d.get("oomkilled_confirmed_utc") or
                   d.get("cleaned_up_utc") or d.get("caught_up_utc") or onset_raw)
        all_eps.append({"cls": "DISCARDED:" + d.get("fault_class", "?"),
                         "run_id": f"{d.get('run_id', '?')} ({f.parent.name})",
                         "onset": onset, "end": iso(end_raw)})
    return all_eps


def nearest_neighbor_bound(all_eps, target_onset):
    candidates = [e for e in all_eps if e["end"] < target_onset]
    if not candidates:
        return None, None
    nearest = max(candidates, key=lambda e: e["end"])
    gap = (target_onset - nearest["end"]).total_seconds()
    return gap, nearest


def severity_timestamps():
    d = json.load(open(REPO / "results" / "baseline-threshold-evidence" / "evaluation_output.json"))
    return {s["run_id"]: iso(s["severity_utc"]) for s in d["disk_pressure"]["severity_detail"]}


def fit_loo_models(df):
    """Returns {held_out_episode_id: (fitted_scaler, fitted_clf)} for the 8 real-episode folds."""
    X = df[DELTA_FEATURE_COLS].values
    y = df["label"].values
    groups = df["episode_id"].values
    logo = LeaveOneGroupOut()
    models = {}
    for train_idx, test_idx in logo.split(X, y, groups):
        held_out = groups[test_idx][0]
        if held_out not in DISK_PRESSURE_EPISODES:
            continue  # skip normal_reference folds -- not relevant to lead-time reconstruction
        scaler = StandardScaler()
        X_train = scaler.fit_transform(X[train_idx])
        y_train = y[train_idx]
        clf = RandomForestClassifier(n_estimators=200, max_depth=5, random_state=42, class_weight="balanced")
        clf.fit(X_train, y_train)
        models[held_out] = (scaler, clf)
    return models


def main():
    print("=== Step 1: corrected nearest-neighbor bounds (any class) ===")
    all_eps = load_all_episodes()
    bounds = {}
    for rid in DISK_PRESSURE_EPISODES:
        target = next(e for e in all_eps if e["cls"] == "ACTIVE:disk_pressure" and e["run_id"] == rid)
        gap, nearest = nearest_neighbor_bound(all_eps, target["onset"])
        if gap is None:
            bound_s = 90  # no episode of any class precedes it in this dataset; documented fallback
            print(f"  {rid:10s}: no preceding episode of ANY class -- true first episode overall, "
                  f"fallback bound={bound_s}s")
        else:
            bound_s = min(gap, MAX_SCAN_S)
            print(f"  {rid:10s}: nearest={nearest['cls']}/{nearest['run_id']} gap={gap:.0f}s -> "
                  f"bound={bound_s:.0f}s")
        bounds[rid] = bound_s

    severities = severity_timestamps()

    print("\n=== Step 2: fit the 8 real-episode LOO delta-feature models ===")
    full_df = pd.read_csv(REPO / "results" / "ml-first-pass" / "extracted_windows.csv")
    sub = full_df[full_df.fault_class == "disk_pressure"].reset_index(drop=True)
    # normal_reference rows use the quiet-period mean baseline, same as disk_pressure_delta_features.py
    QUIET_PERIOD_MEAN = 994899156269.1765
    baselines = sub["episode_id"].map(lambda e: EPISODE_BASELINES.get(e, QUIET_PERIOD_MEAN))
    sub["delta_mean"] = sub["mean"] - baselines
    sub["delta_min"] = sub["min"] - baselines
    sub["delta_max"] = sub["max"] - baselines
    sub["delta_last"] = sub["last"] - baselines
    models = fit_loo_models(sub)
    print(f"  fitted {len(models)} held-out models: {sorted(models.keys())}")

    print("\n=== Step 3: continuous backward scan per episode ===")
    prom = eat.PromClient()
    results = {}
    try:
        for rid in DISK_PRESSURE_EPISODES:
            ep = next(e for e in all_eps if e["cls"] == "ACTIVE:disk_pressure" and e["run_id"] == rid)
            onset = ep["onset"]
            bound_s = bounds[rid]
            scaler, clf = models[rid]
            baseline = EPISODE_BASELINES[rid]
            severity = severities[rid]

            query_start = onset - timedelta(seconds=bound_s + WINDOW_S + 30)
            query_end = onset + timedelta(seconds=30)
            metric = eat.CLASS_CONFIG["disk_pressure"]["metric"].format(pod="")
            samples = prom.query_range(metric, query_start, query_end, step="15s")

            horizons = list(range(STEP_S, int(bound_s) + 1, STEP_S))
            scan_points = []
            for h in horizons:
                win_end = onset - timedelta(seconds=h)
                win_start = win_end - timedelta(seconds=WINDOW_S)
                feats = eat.window_features(samples, win_start, win_end)
                if feats is None:
                    continue
                delta_mean = feats["mean"] - baseline
                delta_min = feats["min"] - baseline
                delta_max = feats["max"] - baseline
                delta_last = feats["last"] - baseline
                X_row = np.array([[delta_mean, delta_min, delta_max, delta_last,
                                    feats["std"], feats["n_samples"]]])
                X_scaled = scaler.transform(X_row)
                pred = int(clf.predict(X_scaled)[0])
                scan_points.append({"horizon_s": h, "window_end_utc": win_end.isoformat(),
                                     "pred": pred, "features": feats})

            positives = [p for p in scan_points if p["pred"] == 1]
            if not positives:
                print(f"  {rid:10s}: NO positive prediction anywhere in {bound_s:.0f}s scan "
                      f"({len(scan_points)} points) -- lead time NOT recoverable")
                results[rid] = {"bound_s": bound_s, "n_scan_points": len(scan_points),
                                 "recoverable": False, "scan_points": scan_points}
                continue

            earliest = max(positives, key=lambda p: p["horizon_s"])
            earliest_window_end = iso(earliest["window_end_utc"])
            lead_time_s = (severity - earliest_window_end).total_seconds()
            flagged = earliest["horizon_s"] > FLAG_HORIZON_S
            flag_str = "  *** FLAGGED (horizon > 120s before onset) ***" if flagged else ""
            print(f"  {rid:10s}: earliest positive at horizon={earliest['horizon_s']}s before onset "
                  f"(window_end={earliest['window_end_utc']}), lead_time={lead_time_s:.0f}s{flag_str}")

            results[rid] = {
                "bound_s": bound_s, "n_scan_points": len(scan_points), "recoverable": True,
                "earliest_horizon_s": earliest["horizon_s"],
                "earliest_window_end_utc": earliest["window_end_utc"],
                "severity_utc": severity.isoformat(),
                "lead_time_s": lead_time_s, "flagged": flagged,
                "scan_points": scan_points,
            }
    finally:
        prom.close()

    with open(REPO / "results" / "ml-first-pass" / "disk_pressure_lead_time.json", "w") as f:
        json.dump(results, f, indent=2, default=str)

    print("\n=== Summary ===")
    recoverable = {k: v for k, v in results.items() if v["recoverable"]}
    print(f"Lead time recoverable for {len(recoverable)}/8 episodes")
    flagged = [k for k, v in recoverable.items() if v.get("flagged")]
    print(f"Flagged for manual trace: {flagged if flagged else 'none'}")


if __name__ == "__main__":
    main()
