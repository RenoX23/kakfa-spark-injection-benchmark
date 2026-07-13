#!/usr/bin/env bash
# Driver for KSPFail: a Kafka+Spark fault-injection benchmark. There is no GUI/web/server
# surface here -- the "app" is a set of Python scripts driven against (a) a live
# Kind/Kubernetes cluster (fault_injection/) and (b) committed CSV/JSON research data
# (modeling/). This wraps both paths with one entrypoint. Run from the repo root
# (the directory containing requirements.txt, fault_injection/, modeling/, infra/).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO_ROOT"
PY=".venv/bin/python"
SKILL_DIR=".claude/skills/run-kspfail"

usage() {
  cat <<'EOF'
Usage: driver.sh <command> [args]

  env                          Create .venv if missing, install requirements.txt, verify imports.
  infra-status                 Read-only: check the Kind cluster + kafka/spark/monitoring pods.
  smoke                        Read-only: verify env + committed data + a real sklearn fit. No side effects.
  modeling-run <script.py>     Run a script from modeling/ against committed data (e.g. disk_pressure_shap.py).
                                MUTATES results/ml-first-pass/*.json -- check `git status` after.
  inject <class> --run-id ID   *** LIVE, CONSEQUENTIAL *** Injects a real fault (kills a pod / fills
                                real disk / OOMs a real container) against the live cluster.
                                <class> is one of: broker_kill, executor_oom, disk_pressure,
                                network_degradation, backpressure_cascade. Extra args pass through.
  topup <class> <target_n>     *** LIVE, CONSEQUENTIAL *** Runs fault_injection/campaign.py's
                                top-up loop for <class> until <target_n> valid records exist.

env / infra-status / smoke / modeling-run are safe to run without asking anyone.
inject / topup run real faults against a live cluster -- do not run them without the
user's explicit go-ahead (this mirrors the project's own .claude/CLAUDE.md pivot/push
rules, which this driver does not override).
EOF
}

cmd_env() {
  if [ ! -x "$PY" ]; then
    echo "[env] .venv missing -- creating"
    python3 -m venv .venv
  fi
  echo "[env] installing requirements.txt into .venv (not system Python)"
  .venv/bin/pip install -q -r requirements.txt
  "$PY" -c "
import numpy, pandas, sklearn, scipy, xgboost, lightgbm, shap, matplotlib
print('numpy', numpy.__version__)
print('pandas', pandas.__version__)
print('scikit-learn', sklearn.__version__)
print('xgboost', xgboost.__version__)
print('lightgbm', lightgbm.__version__)
print('shap', shap.__version__)
print('matplotlib', matplotlib.__version__)
"
  echo "[env] OK"
}

cmd_infra_status() {
  echo "=== node ==="
  kubectl get nodes || { echo "kubectl unreachable -- no cluster. See SKILL.md Prerequisites to build one."; exit 1; }
  echo "=== kafka namespace ==="
  kubectl get pods -n kafka
  echo "=== spark namespace ==="
  kubectl get pods -n spark
  echo "=== monitoring namespace ==="
  kubectl get pods -n monitoring
  echo
  # kubectl only honors the LAST -n flag when given multiple -- confirmed while building
  # this driver (`kubectl get pods -n kafka -n spark -n monitoring` silently only checked
  # monitoring). Loop over namespaces individually instead.
  bad=0
  for ns in kafka spark monitoring; do
    n=$(kubectl get pods -n "$ns" --no-headers 2>/dev/null | grep -v -c "Running" || true)
    bad=$((bad + n))
  done
  if [ "$bad" -gt 0 ]; then
    echo "infra-status: DEGRADED -- $bad pod(s) not Running. fault_injection/campaign.py's"
    echo "SPARK_DEPENDENT_CLASSES path self-heals spark/executor pods (heal_spark()) but"
    echo "won't fix kafka/monitoring; see SKILL.md Troubleshooting."
    exit 1
  fi
  echo "infra-status: OK -- all pods Running"
}

cmd_smoke() {
  "$PY" "$SKILL_DIR/smoke_check.py"
}

cmd_modeling_run() {
  local script="${1:?usage: driver.sh modeling-run <script.py>}"
  if [ ! -f "modeling/$script" ]; then
    echo "modeling/$script not found. Available:"; ls modeling/*.py
    exit 1
  fi
  echo "[modeling-run] .venv/bin/python modeling/$script"
  echo "[modeling-run] WARNING: this will overwrite its output JSON under results/ml-first-pass/"
  echo "[modeling-run] if that file is tracked, check 'git status' after and don't commit an"
  echo "[modeling-run] unintended rewrite of real research evidence."
  "$PY" "modeling/$script"
}

cmd_inject() {
  local cls="${1:?usage: driver.sh inject <class> --run-id ID}"; shift
  echo "*** LIVE FAULT INJECTION: $cls ***  This kills a real pod / fills real disk / OOMs a"
  echo "real container on the cluster kubectl is pointed at. Confirm this is intended before"
  echo "proceeding -- Ctrl-C now to abort."
  sleep 3
  "$PY" "fault_injection/${cls}.py" "$@"
}

cmd_topup() {
  local cls="${1:?usage: driver.sh topup <class> <target_n>}"
  local target="${2:?usage: driver.sh topup <class> <target_n>}"
  echo "*** LIVE CAMPAIGN TOP-UP: $cls -> N=$target ***  Runs real fault injections in a loop"
  echo "until <target_n> valid records exist in results/campaign-n8/$cls/. Confirm this is"
  echo "intended before proceeding -- Ctrl-C now to abort."
  sleep 3
  "$PY" fault_injection/campaign.py --top-up-class "$cls" --top-up-target "$target" \
    --outdir results/campaign-n8
}

case "${1:-}" in
  env) cmd_env ;;
  infra-status) cmd_infra_status ;;
  smoke) cmd_smoke ;;
  modeling-run) shift; cmd_modeling_run "$@" ;;
  inject) shift; cmd_inject "$@" ;;
  topup) shift; cmd_topup "$@" ;;
  *) usage; exit 1 ;;
esac
