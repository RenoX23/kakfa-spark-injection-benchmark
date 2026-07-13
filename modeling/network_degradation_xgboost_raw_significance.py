"""Gate-audit finding (Pass 3, 2026-07-12): Section 6.4 asserted network_degradation's
tuned XGBoost hit "F1=0.941/p<0.01" on the raw (n_samples-included) feature set before
the leak guard de-confounded it, but the codebase's shuffle test only ever ran on the
de-confounded version -- that specific number was asserted from prose, not traced to a
committed file, violating this document's own "every number traceable to a committed
JSON" standard. Computes and commits the missing number: the same significance test
`evaluate_class()` runs automatically, but pinned to the raw 6-feature set and the
already-established mode config (network_degradation.xgboost.mode_config in
multi_model_nested_tuning.json), instead of the auto-selected de-confounded one.

FROZEN HISTORICAL RESULT (Pass-5 gate-audit finding, 2026-07-13): this documents the
n_samples leak at the [15s,30s]/30s config that was superseded the same day (Section
6.3's second correction). The leak does not exist at the current [10s,15s]/15s config
(confirmed: network_degradation's XGBoost tuned F1 there is ~0.385, not 0.941 --
multi_model_nested_tuning.json). Re-running this script against the current
extracted_windows.csv would silently overwrite the historical finding with an unrelated
number under the same filename -- guarded against below by refusing to run unless the
CSV is still at the exact historical config this result documents.
"""
import json
from pathlib import Path

import pandas as pd

from multi_model_nested_tuning import FEATURE_COLS, loo_cv_nested, shuffle_test
from sklearn.metrics import f1_score, precision_score, recall_score

REPO = Path(__file__).resolve().parent.parent
FIXED_CONFIG = {"learning_rate": 0.1, "max_depth": 3, "n_estimators": 100}  # established mode_config
HISTORICAL_HORIZONS_S = {15.0, 30.0}  # the [15s,30s]/30s config this frozen result documents


def main():
    df = pd.read_csv(REPO / "results" / "ml-first-pass" / "extracted_windows.csv")
    sub = df[df.fault_class == "network_degradation"].reset_index(drop=True)
    current_horizons = set(sub["horizon_s"].dropna().unique())
    if current_horizons != HISTORICAL_HORIZONS_S:
        raise SystemExit(
            f"extracted_windows.csv is at horizons={current_horizons}, not the "
            f"[15s,30s]/30s config this script's frozen historical result documents. "
            f"Refusing to overwrite results/ml-first-pass/"
            f"network_degradation_xgboost_raw_significance.json with a number from a "
            f"different config under the same filename -- see this file's module "
            f"docstring."
        )
    X = sub[FEATURE_COLS].values
    y = sub["label"].values
    groups = sub["episode_id"].values

    true, pred, _ = loo_cv_nested("xgboost", X, y, groups, tune=False, fixed_config=FIXED_CONFIG)
    f1 = f1_score(true, pred, zero_division=0)
    precision = precision_score(true, pred, zero_division=0)
    recall = recall_score(true, pred, zero_division=0)
    print(f"raw (n_samples-included) F1={f1:.3f} precision={precision:.3f} recall={recall:.3f}")

    sig = shuffle_test("xgboost", X, y, groups, FIXED_CONFIG, f1)
    print(f"significance: {sig['n_shuffled_ge_real']}/100 -> {sig['p_value_reported']}")

    result = {"features_used": FEATURE_COLS, "fixed_config": FIXED_CONFIG,
              "precision": precision, "recall": recall, "f1": f1, **sig,
              "historical_note": "Computed at the superseded [15s,30s]/30s config (Section 6.3's "
                                  "second correction replaced it with [10s,15s]/15s same day). This "
                                  "n_samples leak does not reproduce at the current config -- frozen "
                                  "as a historical record, not a live result."}
    with open(REPO / "results" / "ml-first-pass" / "network_degradation_xgboost_raw_significance.json", "w") as f:
        json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()
