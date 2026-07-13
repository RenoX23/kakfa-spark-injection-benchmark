---
name: run-kspfail
description: Build, run, and drive KSPFail (Kafka+Spark fault-injection benchmark for failure-prediction research). Use when asked to run KSPFail, check the cluster, run the modeling pipeline, inject a fault, or re-run a SHAP/LOO-CV/significance check.
---

KSPFail has no GUI/web/server surface — it is a Kubernetes-native
fault-injection benchmark driven entirely by Python scripts: one set
(`fault_injection/`) injects real faults into a live Kind cluster
(Kafka + Spark Structured Streaming + Prometheus), another
(`modeling/`) runs ML evaluation against the committed CSV/JSON
results those injections produced. Drive it via
`.claude/skills/run-kspfail/driver.sh` — it wraps both paths. All
paths below are relative to the repo root (the directory containing
`requirements.txt`, `fault_injection/`, `modeling/`, `infra/`).

## Prerequisites

- `kubectl` on PATH, pointed at a reachable cluster (a Kind cluster
  named `kspfail-control-plane` in this project's case — `kind get
  clusters` to check, `infra/kind/kind-config.yaml` to recreate one if
  none exists).
- Python 3.12 (`python3 -m venv` must work).
- No system Python packages needed or wanted — everything installs
  into `.venv`, gitignored, created at repo root
  (`.claude/CLAUDE.md`'s non-negotiable rule; the driver enforces this
  by construction — it only ever calls `.venv/bin/python`/`pip`).

If no cluster exists yet, the infra is declarative and rebuildable
from `infra/`: Kind (`infra/kind/kind-config.yaml`) → Strimzi Kafka
operator + `infra/kafka/kafka-single-broker.yaml` → Prometheus
(`infra/prometheus/values.yaml`, via the `prometheus-community` Helm
chart) → Spark (`kubectl apply -f infra/spark/submit-pod.yaml`, which
also creates the two ConfigMaps it needs). This was not re-run to
build this skill — the cluster in this container was already up and
healthy (`driver.sh infra-status` below is what proved that, live,
not assumed) — so treat the rebuild sequence as documented-but-
unverified-this-session, per the project's own evidence standard.

## Setup

```bash
.claude/skills/run-kspfail/driver.sh env
```

Creates `.venv` if missing, installs `requirements.txt` into it, and
prints every installed package's version as a verification step (not
just "pip didn't error"). Ran clean in this container:

```
numpy 2.4.6
pandas 3.0.3
scikit-learn 1.9.0
xgboost 3.3.0
lightgbm 4.6.0
shap 0.52.0
matplotlib 3.11.0
```

## Run (agent path)

`driver.sh` has six subcommands. The first four are **read-only /
side-effect-free** and safe to run without asking anyone; the last two
inject **real faults against a live cluster** and must not be run
without the user's explicit go-ahead — this mirrors
`.claude/CLAUDE.md`'s own pivot/push discipline, which this driver
does not override or bypass.

| command | effect | safe to run freely? |
|---|---|---|
| `env` | create/verify `.venv` | yes |
| `infra-status` | `kubectl get nodes` + pods in `kafka`/`spark`/`monitoring` namespaces | yes, read-only |
| `smoke` | verify env + committed data + a real sklearn LOO-CV fit, writes nothing | yes, writes nothing |
| `modeling-run <script.py>` | run any `modeling/*.py` script for real | **mutates tracked JSON under `results/ml-first-pass/`** — check `git status` after |
| `inject <class> --run-id ID` | inject one real fault (kills a pod / fills disk / OOMs a container) | **no** — live, consequential |
| `topup <class> <target_n>` | loop real fault injections until `<class>` has `<target_n>` valid records | **no** — live, consequential |

```bash
.claude/skills/run-kspfail/driver.sh infra-status
# → node Ready, kafka/spark/monitoring pods all Running, exit 0
#   (exit 1 + "DEGRADED" if any pod isn't Running)

.claude/skills/run-kspfail/driver.sh smoke
# → [4/4] SMOKE CHECK: PASS -- environment + committed data + sklearn fit all work
```

`smoke` is the fast, default way to confirm the whole stack (env +
committed research data + sklearn) actually works — it does **not**
run any of the real `modeling/*.py` scripts (see Gotchas: they all
overwrite committed evidence files, so running one is a deliberate
research action, not a smoke test). It reads
`results/ml-first-pass/extracted_windows.csv`, checks all 4 modeled
fault classes are present, and fits one throwaway LOO-CV model
in-memory. Source: `.claude/skills/run-kspfail/smoke_check.py`.

To actually re-run a real modeling check (e.g. after editing a
script):

```bash
.claude/skills/run-kspfail/driver.sh modeling-run disk_pressure_shap.py
git status --short   # confirm only the file(s) you meant to change are dirty
```

## Run (human path)

Same commands — there is no separate human path. A human would `cd`
into the repo and run `.venv/bin/python modeling/<script>.py` or
`.venv/bin/python fault_injection/<class>.py --run-id <id>` directly;
the driver is a thin, safety-labeled wrapper around exactly that, not
a different interface.

## Test

There is no separate test suite — the "tests" *are* the modeling
scripts' own built-in checks (LOO-CV + trivial-baseline + rank-based
permutation/shuffle test, run inline, result written to a JSON and
printed). `driver.sh smoke` is the closest thing to a health check;
`driver.sh modeling-run <script>.py` re-runs a specific real check.

---

## Gotchas

- **`kubectl get pods -n kafka -n spark -n monitoring` silently only
  checks the *last* namespace.** kubectl does not merge multiple `-n`
  flags — it just uses whichever one comes last. Confirmed directly
  while building this driver: that one-liner returned only the
  `monitoring` namespace's 3 pods, silently dropping kafka and spark
  from the check. `driver.sh infra-status` loops over the three
  namespaces individually instead — don't "simplify" it back to one
  multi-`-n` call.
- **Do not use any `modeling/*.py` script as a smoke test.** Every one
  writes its result to a fixed, committed path under
  `results/ml-first-pass/` (e.g.
  `executor_oom_feature_importance_check.py` always overwrites
  `executor_oom_feature_importance_check.json`). Confirmed concretely:
  running that specific script against the *current* committed CSV
  (post the 2026-07-13 executor_oom N=8→N=15 top-up) produces a
  **different** result than what's committed, because the script's
  own `CLEAN_EPISODES` list predates the top-up — a real, pre-existing
  staleness this session surfaced as a side effect, not something this
  skill fixes. `driver.sh smoke` exists specifically so "does the env
  work" doesn't require touching real research evidence.
- **`inject`/`topup` are not idempotent and not reversible.** They
  kill real pods, fill real disk (`disk_pressure`, small and bounded
  but real), and OOM-kill real Spark executors. `fault_injection/campaign.py`
  self-heals Spark driver/executor pods on crash (`heal_spark()`), but
  that's a mitigation for its own side effects, not a reason to treat
  these commands as harmless. Both `driver.sh` subcommands print a
  warning and sleep 3s before proceeding, but that is not a substitute
  for asking first.
- **numpy got downgraded 2.5.1 → 2.4.6** installing `shap` (its own
  dependency ceiling). Re-verified numpy/pandas/scikit-learn/shap all
  still interoperate (`driver.sh env`'s version-print step, and
  `smoke`'s real LOO-CV fit) — don't assume a fresh `pip install -r
  requirements.txt` reproduces exactly 2.5.1; 2.4.6 is correct and
  pinned.

## Troubleshooting

- **`infra-status` prints `DEGRADED`**: one or more pods aren't
  `Running`. For `spark` namespace specifically, this project's own
  `fault_injection/campaign.py` has a `heal_spark()` function (delete
  driver/executor/submit-runner pods, `kubectl apply -f
  infra/spark/submit-pod.yaml`) — that's the documented fix, not a
  manual pod restart. For `kafka`/`monitoring`, there's no auto-heal;
  check `kubectl describe pod -n <ns> <pod>` for the actual cause.
- **`kubectl get nodes` fails / no cluster**: no Kind cluster is up.
  See Prerequisites above for the rebuild sequence from `infra/` — not
  re-verified this session, since a working cluster was already
  present.
- **`smoke` fails at `[2/4]` with "extracted_windows.csv does not
  exist"**: no modeling pass has been run yet. Needs a live cluster
  (`infra-status` first) and `.venv/bin/python
  modeling/extract_and_train.py`, which itself queries Prometheus for
  each fault class's real episode windows — not something `driver.sh`
  wraps, since it's a one-time bootstrap step, not a repeatable check.
