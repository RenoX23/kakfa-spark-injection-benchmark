"""Read-only smoke check for the KSPFail modeling stack -- run by driver.sh smoke.

Deliberately does NOT call any of the real modeling/*.py scripts directly: every one
of them writes its result to a fixed path under results/ml-first-pass/ (e.g.
`modeling/executor_oom_feature_importance_check.py` always overwrites
`executor_oom_feature_importance_check.json`), and those files are committed research
evidence, cited by line number from docs/research_context.md. Running one as a casual
"does the env work" check would silently rewrite real results and dirty the tree --
confirmed concretely while building this skill: `executor_oom_feature_importance_check.py`
run against the current (N=15, post-2026-07-13-topup) extracted_windows.csv produces a
different result than the committed JSON, because the script's own CLEAN_EPISODES list
predates the topup. That's a pre-existing staleness worth flagging separately, not
something this smoke check should paper over by treating the mutation as normal.

This script instead reads the committed CSV and does its own tiny, throwaway LOO-CV fit
that writes nothing -- it proves the environment (numpy/pandas/scikit-learn/shap) and
the real committed data are both usable, without touching any tracked file.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import LeaveOneGroupOut
from sklearn.metrics import f1_score
from sklearn.preprocessing import StandardScaler

REPO = Path(__file__).resolve().parent.parent.parent.parent
CSV_PATH = REPO / "results" / "ml-first-pass" / "extracted_windows.csv"
EXPECTED_CLASSES = {"broker_kill", "executor_oom", "disk_pressure", "network_degradation"}
FEATURE_COLS = ["mean", "std", "min", "max", "last", "n_samples"]


def fail(msg):
    print(f"SMOKE CHECK: FAIL -- {msg}")
    sys.exit(1)


def main():
    print(f"[1/4] imports: numpy {np.__version__}, pandas {pd.__version__}")
    try:
        import shap  # noqa: F401
        import matplotlib  # noqa: F401
        print(f"       shap {shap.__version__}, matplotlib {matplotlib.__version__}")
    except ImportError as e:
        fail(f"optional explainability deps missing ({e}) -- ok for core modeling, "
             f"but disk_pressure_shap.py needs `pip install -r requirements.txt` in .venv")

    print(f"[2/4] reading {CSV_PATH.relative_to(REPO)}")
    if not CSV_PATH.exists():
        fail(f"{CSV_PATH} does not exist -- run modeling/extract_and_train.py first "
             f"(needs a live cluster, see SKILL.md's infra-status)")
    df = pd.read_csv(CSV_PATH)
    present = set(df.fault_class.unique())
    missing = EXPECTED_CLASSES - present
    if missing:
        fail(f"extracted_windows.csv is missing expected fault classes: {missing} "
             f"(present: {present})")
    print(f"       {len(df)} rows, classes present: {sorted(present)}")

    print("[3/4] fitting a throwaway LOO-CV RandomForest on disk_pressure (fast, ~1s, "
          "writes nothing)")
    sub = df[df.fault_class == "disk_pressure"].reset_index(drop=True)
    X = sub[FEATURE_COLS].values
    y = sub["label"].values
    groups = sub["episode_id"].values
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
            clf = RandomForestClassifier(n_estimators=50, max_depth=5, random_state=42)
            clf.fit(X_train, y_train)
            preds = clf.predict(X_test)
        all_true.extend(y[test_idx].tolist())
        all_pred.extend(preds.tolist())
    f1 = f1_score(all_true, all_pred, zero_division=0)
    print(f"       LOO-CV fit ok, {len(sub)} rows across {sub.episode_id.nunique()} "
          f"groups, raw-feature F1={f1:.3f} (not the delta-feature result reported in "
          f"the paper -- this is a smoke check, not a re-derivation)")

    print("[4/4] SMOKE CHECK: PASS -- environment + committed data + sklearn fit all work")


if __name__ == "__main__":
    main()
