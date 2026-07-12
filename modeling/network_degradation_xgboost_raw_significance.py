"""Gate-audit finding (Pass 3, 2026-07-12): Section 6.4 asserted network_degradation's
tuned XGBoost hit "F1=0.941/p<0.01" on the raw (n_samples-included) feature set before
the leak guard de-confounded it, but the codebase's shuffle test only ever ran on the
de-confounded version -- that specific number was asserted from prose, not traced to a
committed file, violating this document's own "every number traceable to a committed
JSON" standard. Computes and commits the missing number: the same significance test
`evaluate_class()` runs automatically, but pinned to the raw 6-feature set and the
already-established mode config (network_degradation.xgboost.mode_config in
multi_model_nested_tuning.json), instead of the auto-selected de-confounded one.
"""
import json
from pathlib import Path

import pandas as pd

from multi_model_nested_tuning import FEATURE_COLS, loo_cv_nested, shuffle_test
from sklearn.metrics import f1_score, precision_score, recall_score

REPO = Path(__file__).resolve().parent.parent
FIXED_CONFIG = {"learning_rate": 0.1, "max_depth": 3, "n_estimators": 100}  # established mode_config


def main():
    df = pd.read_csv(REPO / "results" / "ml-first-pass" / "extracted_windows.csv")
    sub = df[df.fault_class == "network_degradation"].reset_index(drop=True)
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
              "precision": precision, "recall": recall, "f1": f1, **sig}
    with open(REPO / "results" / "ml-first-pass" / "network_degradation_xgboost_raw_significance.json", "w") as f:
        json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()
