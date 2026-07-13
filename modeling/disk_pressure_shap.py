"""Section 6.6 (Explainability) -- SHAP analysis on disk_pressure's delta-feature
LOO-CV models: the F1=0.941, 0/100 shuffled>=real (p<0.01) classifier from
disk_pressure_delta_features.py, the one significance-tested positive result in this
modeling pass.

This is a cross-check, not a fresh exploration: Weeks 10-11's per-instance ablation
(disk_pressure_lead_time_ablation.py) already found the lead-time-reconstruction
model's decision boundary sits at a static ~-350,000-byte delta_mean/min/max/last
threshold with zero cross-fold variance (std_threshold=0.0), and the mechanism trace
(research_context.md Section 6.5) concluded this is a static absolute-value proximity
check, not an escalating physical precursor -- there is no rate-of-change feature
anywhere in this pipeline (DELTA_FEATURE_COLS has no slope/derivative term). SHAP here
explains the actual reported classification result (F1=0.941), using the identical
18-fold LeaveOneGroupOut setup (same X/y/groups, same RandomForestClassifier
hyperparameters, same random_state=42, same per-fold StandardScaler) as
disk_pressure_delta_features.py's loo_cv_eval -- not a different model standing in for
it. If SHAP's attribution disagrees with the ablation's static-proximity finding (e.g.
attributes real weight to std or n_samples, which shouldn't matter under a pure
magnitude-threshold explanation), that is a genuine discrepancy to report, not to
resolve by picking whichever explanation is convenient.

shap.TreeExplainer is exact for tree ensembles (no sampling approximation). SHAP values
are computed in the SAME scaled feature space the classifier actually sees (each fold's
StandardScaler, fit on that fold's training data only) -- mechanistically correct since
TreeExplainer walks the real trained tree structure, and per-feature scaling is a
monotonic transform that preserves relative attribution ranking across features.
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shap
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import LeaveOneGroupOut
from sklearn.preprocessing import StandardScaler

# Gate, per the mechanism-trace cross-check this script exists to run: only produce the
# paper figure if magnitude features (delta_mean/min/max/last) dominate attribution and
# no non-magnitude feature (std, n_samples) carries a share that would suggest something
# other than a static proximity check is driving the decision. Thresholds are
# deliberately generous toward *catching* a discrepancy, not toward passing the gate --
# 80% is well below the ablation's own near-100% expectation, and 15% for a single
# non-magnitude feature is well above the noise floor a genuine non-finding should show.
MAGNITUDE_SHARE_GATE = 0.80
NONMAGNITUDE_SINGLE_FEATURE_GATE = 0.15

REPO = Path(__file__).resolve().parent.parent

EPISODE_BASELINES = {
    "campaign1": 995152609280.0, "campaign2": 995152027648.0, "campaign3": 995150802944.0,
    "campaign4": 995149934592.0, "campaign5": 995148972032.0, "campaign7": 995146944512.0,
    "campaign8": 995145748480.0, "topup1": 995050299392.0,
}
QUIET_PERIOD_MEAN = 994899156269.1765
DELTA_FEATURE_COLS = ["delta_mean", "delta_min", "delta_max", "delta_last", "std", "n_samples"]
REAL_EPISODES = list(EPISODE_BASELINES.keys())
MAGNITUDE_FEATURES = {"delta_mean", "delta_min", "delta_max", "delta_last"}


def baseline_for(episode_id):
    return EPISODE_BASELINES.get(episode_id, QUIET_PERIOD_MEAN)


def main():
    df = pd.read_csv(REPO / "results" / "ml-first-pass" / "extracted_windows.csv")
    sub = df[df.fault_class == "disk_pressure"].reset_index(drop=True)
    baselines = sub["episode_id"].map(baseline_for)
    sub["delta_mean"] = sub["mean"] - baselines
    sub["delta_min"] = sub["min"] - baselines
    sub["delta_max"] = sub["max"] - baselines
    sub["delta_last"] = sub["last"] - baselines

    X = sub[DELTA_FEATURE_COLS].values
    y = sub["label"].values
    groups = sub["episode_id"].values
    logo = LeaveOneGroupOut()

    per_instance = []
    per_fold_diagnostics = {}
    for train_idx, test_idx in logo.split(X, y, groups):
        held_out = groups[test_idx][0]
        if held_out not in REAL_EPISODES:
            continue  # only the 8 real-episode folds -- normal_reference folds have no pre_failure instance to explain

        scaler = StandardScaler()
        X_train = scaler.fit_transform(X[train_idx])
        X_test = scaler.transform(X[test_idx])
        y_train = y[train_idx]

        clf = RandomForestClassifier(n_estimators=200, max_depth=5, random_state=42, class_weight="balanced")
        clf.fit(X_train, y_train)
        preds = clf.predict(X_test)

        # Persisted, not asserted from prose: the "coarse-threshold" claim below rests on
        # folds being genuinely distinct fits (different training data -> different
        # scaler/tree) that nonetheless agree on every real instance's leaf path. Record
        # the actual scaler mean_ and the first tree's raw structure per fold so that
        # claim is independently checkable from committed data, not a one-off shell check
        # that never got persisted (the exact gap this repo's gate-auditor has flagged
        # before, e.g. disk_pressure_lead_time_ablation.py's origin story).
        tree0 = clf.estimators_[0].tree_
        per_fold_diagnostics[held_out] = {
            "scaler_mean_": {c: float(scaler.mean_[j]) for j, c in enumerate(DELTA_FEATURE_COLS)},
            "n_train_rows": int(len(train_idx)),
            "tree0_feature": tree0.feature.tolist(),
            "tree0_threshold": tree0.threshold.tolist(),
        }

        explainer = shap.TreeExplainer(clf)
        sv = explainer.shap_values(X_test)
        # sklearn RF binary classifier: shap_values shape is (n_samples, n_features, n_classes)
        # in recent shap versions -- select class-1 (pre_failure) contributions explicitly.
        if isinstance(sv, list):
            sv1 = sv[1]
        elif sv.ndim == 3:
            sv1 = sv[:, :, 1]
        else:
            sv1 = sv

        raw_rows = sub.iloc[test_idx]
        for i in range(len(test_idx)):
            row = raw_rows.iloc[i]
            per_instance.append({
                "episode_id": held_out,
                "horizon_s": None if pd.isna(row.get("horizon_s")) else row.get("horizon_s"),
                "true_label": int(y[test_idx[i]]),
                "pred_label": int(preds[i]),
                "correctly_classified": bool(y[test_idx[i]] == preds[i]),
                "raw_feature_values": {c: float(row[c]) for c in DELTA_FEATURE_COLS},
                "scaled_feature_values": {c: float(X_test[i, j]) for j, c in enumerate(DELTA_FEATURE_COLS)},
                "shap_values_class1": {c: float(sv1[i, j]) for j, c in enumerate(DELTA_FEATURE_COLS)},
                "shap_base_value_class1": float(
                    explainer.expected_value[1] if isinstance(explainer.expected_value, (list, np.ndarray))
                    else explainer.expected_value
                ),
            })

    print(f"Explained {len(per_instance)} held-out instances across {len(REAL_EPISODES)} real-episode folds")
    tp = [r for r in per_instance if r["true_label"] == 1 and r["pred_label"] == 1]
    print(f"True-positive (correctly classified pre_failure) instances: {len(tp)}")

    print("\n=== per-instance SHAP breakdown (true-positive pre_failure instances) ===")
    for r in tp:
        ranked = sorted(r["shap_values_class1"].items(), key=lambda kv: -abs(kv[1]))
        top = ", ".join(f"{k}={v:+.3f}" for k, v in ranked)
        raw_delta = r["raw_feature_values"]["delta_mean"]
        print(f"  {r['episode_id']:10s} h={r['horizon_s']}: raw delta_mean={raw_delta:+,.0f}B  shap(desc)=[{top}]")

    # Aggregate: mean |SHAP| per feature across all true-positive pre_failure instances --
    # this answers "which feature(s) actually drove the classification decision," not just
    # training-time aggregate importance.
    agg = {c: float(np.mean([abs(r["shap_values_class1"][c]) for r in tp])) for c in DELTA_FEATURE_COLS}
    ranked_agg = sorted(agg.items(), key=lambda kv: -kv[1])
    total = sum(agg.values())
    print("\n=== mean |SHAP| per feature, true-positive pre_failure instances (class-1 attribution) ===")
    for feat, val in ranked_agg:
        pct = 100 * val / total if total else 0.0
        print(f"  {feat:12s} mean|shap|={val:.4f}  ({pct:.1f}% of total attribution)")

    magnitude_share = sum(v for k, v in agg.items() if k in MAGNITUDE_FEATURES) / total if total else 0.0
    nonmagnitude_top = max(
        ((k, v) for k, v in agg.items() if k not in MAGNITUDE_FEATURES), key=lambda kv: kv[1]
    )
    print(f"\nmagnitude features (delta_mean/min/max/last) combined share: {magnitude_share*100:.1f}%")
    print(f"largest non-magnitude feature: {nonmagnitude_top[0]}={nonmagnitude_top[1]:.4f} "
          f"({100*nonmagnitude_top[1]/total:.1f}%)")

    # Sign check: does a NEGATIVE delta_mean (decline below baseline) push toward class 1
    # (pre_failure), consistent with a magnitude-threshold direction, or is the sign
    # inconsistent/unrelated to the raw value's direction? Vacuously 0/N if no
    # true-positive instance actually has a negative delta_mean -- flagged explicitly so
    # a reader doesn't misread "0/16" as a failed check rather than an inapplicable one.
    n_negative_delta_mean = sum(1 for r in tp if r["raw_feature_values"]["delta_mean"] < 0)
    sign_consistent = sum(
        1 for r in tp
        if (r["raw_feature_values"]["delta_mean"] < 0) == (r["shap_values_class1"]["delta_mean"] > 0)
    )
    sign_check_applicable = n_negative_delta_mean > 0
    print(f"\nsign check: negative delta_mean -> positive (pre_failure-pushing) SHAP contribution "
          f"in {sign_consistent}/{len(tp)} true-positive instances "
          f"({'APPLICABLE' if sign_check_applicable else 'VACUOUS -- 0/16 true-positive instances have a negative delta_mean at all, see raw_feature_values'})")

    # Coarse-threshold diagnostic: persist a direct pairwise comparison (not just the
    # per-fold arrays above) proving folds are genuinely distinct fits whose SHAP
    # attribution nonetheless coincides on real data -- the specific claim gate-audit
    # asked to see backed by committed evidence, not a one-off shell comparison.
    pair_diagnostics = []
    seen_pairs = set()
    for r1 in tp:
        for r2 in tp:
            if r1["episode_id"] >= r2["episode_id"]:
                continue
            key = (r1["episode_id"], r1["horizon_s"], r2["episode_id"], r2["horizon_s"])
            if key in seen_pairs:
                continue
            shap_close = all(
                abs(r1["shap_values_class1"][c] - r2["shap_values_class1"][c]) < 1e-3
                for c in DELTA_FEATURE_COLS
            )
            if not shap_close:
                continue
            seen_pairs.add(key)
            d1 = per_fold_diagnostics[r1["episode_id"]]
            d2 = per_fold_diagnostics[r2["episode_id"]]
            scaler_differs = any(
                abs(d1["scaler_mean_"][c] - d2["scaler_mean_"][c]) > 1.0 for c in DELTA_FEATURE_COLS
            )
            threshold_differs = d1["tree0_threshold"] != d2["tree0_threshold"]
            pair_diagnostics.append({
                "episode_a": r1["episode_id"], "horizon_a": r1["horizon_s"],
                "episode_b": r2["episode_id"], "horizon_b": r2["horizon_s"],
                "raw_delta_mean_a": r1["raw_feature_values"]["delta_mean"],
                "raw_delta_mean_b": r2["raw_feature_values"]["delta_mean"],
                "shap_vectors_near_identical": True,
                "scaler_mean_genuinely_differs": scaler_differs,
                "tree0_threshold_genuinely_differs": threshold_differs,
            })
    print(f"\ncoarse-threshold diagnostic: {len(pair_diagnostics)} instance pair(s) with near-identical "
          f"SHAP vectors despite different raw delta_mean -- checked whether the underlying "
          f"fold fits (scaler mean_, tree0 thresholds) are genuinely different, not reused:")
    for p in pair_diagnostics:
        print(f"  {p['episode_a']}(h={p['horizon_a']}, delta={p['raw_delta_mean_a']:+,.0f}B) vs "
              f"{p['episode_b']}(h={p['horizon_b']}, delta={p['raw_delta_mean_b']:+,.0f}B): "
              f"scaler_differs={p['scaler_mean_genuinely_differs']} "
              f"tree0_threshold_differs={p['tree0_threshold_genuinely_differs']}")

    nonmagnitude_share = nonmagnitude_top[1] / total if total else 0.0
    corroborated = magnitude_share >= MAGNITUDE_SHARE_GATE and nonmagnitude_share < NONMAGNITUDE_SINGLE_FEATURE_GATE

    out = {
        "n_instances_explained": len(per_instance),
        "n_true_positive_pre_failure": len(tp),
        "mean_abs_shap_per_feature": agg,
        "ranked_features": ranked_agg,
        "magnitude_features": sorted(MAGNITUDE_FEATURES),
        "magnitude_feature_combined_share": magnitude_share,
        "largest_non_magnitude_feature": {"feature": nonmagnitude_top[0], "mean_abs_shap": nonmagnitude_top[1],
                                           "share": nonmagnitude_share},
        "sign_check_negative_delta_pushes_positive_class": {
            "consistent": sign_consistent, "total": len(tp),
            "applicable": sign_check_applicable,
            "note": "vacuous if applicable=false -- no true-positive instance has a negative delta_mean, "
                    "so this check has nothing to evaluate; not a failed check.",
        },
        "gate": {
            "magnitude_share_threshold": MAGNITUDE_SHARE_GATE,
            "nonmagnitude_single_feature_threshold": NONMAGNITUDE_SINGLE_FEATURE_GATE,
            "corroborated": corroborated,
        },
        "coarse_threshold_pair_diagnostics": pair_diagnostics,
        "per_fold_diagnostics": per_fold_diagnostics,
        "per_instance": per_instance,
    }
    out_path = REPO / "results" / "ml-first-pass" / "disk_pressure_shap.json"
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\nwrote {out_path}")

    print(f"\n=== GATE CHECK ===")
    print(f"magnitude_share={magnitude_share:.3f} (need >= {MAGNITUDE_SHARE_GATE}), "
          f"largest_nonmagnitude_share={nonmagnitude_share:.3f} (need < {NONMAGNITUDE_SINGLE_FEATURE_GATE})")
    if not corroborated:
        print("GATE: DISCREPANCY -- SHAP attribution does not match the static-proximity mechanism "
              "finding. NOT producing a figure. Report this, do not smooth over it.")
        return
    print("GATE: CORROBORATED -- magnitude features dominate, no non-magnitude feature carries "
          "meaningful weight. Producing the summary figure.")

    # One clean summary figure: mean |SHAP| per feature (bar) + per-instance beeswarm,
    # side by side -- the bar answers "which feature," the beeswarm shows it holds
    # consistently across all 16 true-positive instances, not just on average.
    feat_order = [f for f, _ in ranked_agg]
    shap_matrix = np.array([[r["shap_values_class1"][f] for f in feat_order] for r in tp])
    raw_matrix = np.array([[r["raw_feature_values"][f] for f in feat_order] for r in tp])

    fig, (ax_bar, ax_bee) = plt.subplots(1, 2, figsize=(11, 4.5))

    bar_vals = [agg[f] for f in feat_order]
    ax_bar.barh(range(len(feat_order)), bar_vals[::-1], color="#4c72b0")
    ax_bar.set_yticks(range(len(feat_order)))
    ax_bar.set_yticklabels(feat_order[::-1])
    ax_bar.set_xlabel("mean |SHAP value|")
    ax_bar.set_title("(a) Mean attribution, class = pre_failure")

    explanation = shap.Explanation(
        values=shap_matrix, data=raw_matrix, feature_names=feat_order,
    )
    plt.sca(ax_bee)
    shap.plots.beeswarm(explanation, show=False, plot_size=None)
    ax_bee.set_title("(b) Per-instance SHAP (n=16 true-positive windows)")

    fig.suptitle(
        "disk_pressure delta-feature classifier (F1=0.941, p<0.01): SHAP attribution\n"
        "confirms a static magnitude/proximity check, not an escalating trend "
        "(no rate-of-change feature in the pipeline)",
        fontsize=10,
    )
    fig.tight_layout(rect=[0, 0, 1, 0.90])
    fig_path = REPO / "results" / "ml-first-pass" / "disk_pressure_shap_summary.png"
    fig.savefig(fig_path, dpi=200)
    print(f"wrote {fig_path}")


if __name__ == "__main__":
    main()
