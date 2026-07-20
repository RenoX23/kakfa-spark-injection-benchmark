# KSPFail — Kafka–Spark Fault-Injection Benchmark for Lead-Time Failure Prediction

A reproducible benchmark that physically injects five real fault classes into a live
**Kafka + Spark Structured Streaming** pipeline, records ground-truth onset timestamps
at the moment each fault is caused, and asks a specific question: **can supervised ML
on pre-failure telemetry warn earlier than the static-threshold alerting rules
operators actually use today, and by how much?**

The honest answer this benchmark found: no. Across five fault classes, only one
produces a classification signal that clears a permutation-test significance check
(disk_pressure, F1 = 0.941, p < 0.01) — and mechanism verification (per-instance
ablation + SHAP) shows that signal is a static absolute-value discriminator, not
detection of a physical precursor. No class yields a machine-learning lead-time
advantage over the threshold baseline. That null result, reported with the same rigor
as a positive one, is the paper's actual contribution — see
[`docs/paper_full_draft.md`](docs/paper_full_draft.md).

## What this repository contains

| Directory | Contents |
|---|---|
| [`infra/`](infra/) | Declarative infrastructure-as-code for the system under test: a single-node Kind cluster, Strimzi-managed Kafka broker (4.3.0) with a JMX exporter, Spark 4.1.2 Structured Streaming job, and Prometheus (60s scrape interval across JMX, cAdvisor, and node-exporter). |
| [`fault_injection/`](fault_injection/) | One script per fault class (`broker_kill.py`, `executor_oom.py`, `disk_pressure.py`, `network_degradation.py`, `backpressure_cascade.py`) plus `campaign.py`, the orchestrator that runs repeated injections and records ground truth. |
| [`modeling/`](modeling/) | Feature extraction, LOO-CV training, multi-model comparison (Random Forest / XGBoost / LightGBM), significance testing (rank-based permutation), lead-time reconstruction, and the mechanism-verification scripts (per-instance ablation, SHAP) that caught this study's one false positive. |
| [`results/`](results/) | All raw and processed evidence: per-episode ground-truth JSON (`campaign-n8/`), the labeled feature table (`ml-first-pass/extracted_windows.csv`), the static-threshold baseline evaluation (`baseline-threshold-evidence/`), and the publication figures (`figures/`). Start with [`results/DATASET.md`](results/DATASET.md) for the dataset card. |
| [`literature/`](literature/) | A curated, primary-source-verified set of 20 related papers across 5 themes, with BibTeX (`references.bib`) and a synthesis matrix (`LITERATURE_MATRIX.md`). PDFs are kept locally (gitignored) for copyright reasons; the index is tracked. |
| [`docs/`](docs/) | `research_context.md` (the full locked-scope research log — methodology, objectives, decisions, and their justifications), `baseline_thresholds.md` (per-class threshold derivation), `paper_full_draft.md` (the content-complete paper draft), `paper_tables.md` (manuscript tables). |
| [`IEEE-Template/`](IEEE-Template/) | The paper in IEEE two-column conference LaTeX format (`kspfail.tex`), with bundled figures. |

## The dataset

**47 active fault episodes, 20 discarded** (every exclusion has a documented reason —
none silently dropped), yielding **102 labeled telemetry windows** across four modeled
classes:

| Fault class | Active | Discarded | Injection mechanism |
|---|:---:|:---:|---|
| broker_kill | 8 | 0 | Delete the Kafka broker pod |
| executor_oom | 15 | 10 | Gradual memory ramp to a Kubernetes OOM kill |
| disk_pressure | 8 | 1 | One-shot 3 GB fill of the node filesystem |
| network_degradation | 8 | 8 | `tc netem` delay + jitter + 10% loss |
| backpressure_cascade | 8 | 1 | Burst load → consumer lag (excluded from modeling — see below) |

backpressure_cascade is injected and its ground truth recorded, but not part of the
modeling set: no independently-scraped Prometheus metric reflects Spark processing lag
on this deployment, so any detector built from the same field the ground truth itself
uses would be circular. Full schema and reproduction instructions:
[`results/DATASET.md`](results/DATASET.md).

## Reproducing this work

1. **Infrastructure**: `kubectl apply -f` the manifests under `infra/` in the order
   documented in `infra/spark/submit-pod.yaml`, against a Kind cluster matching
   `infra/kind/kind-config.yaml`.
2. **Environment**: this project uses a project-local virtualenv, not system Python.
   ```
   python3 -m venv .venv
   .venv/bin/pip install -r requirements.txt
   ```
3. **Fault injection**: `fault_injection/campaign.py` orchestrates repeated real
   injections against the live cluster; `--top-up-class <class> --top-up-target <n>`
   extends any class's episode count.
4. **Modeling**: `modeling/extract_and_train.py` extracts windowed features and runs
   the baseline LOO-CV pipeline; `modeling/multi_model_nested_tuning.py` runs the
   three-classifier comparison.

The existing 47+20 episodes' raw ground truth and extracted features
(`results/ml-first-pass/extracted_windows.csv`) are static and usable for modeling
without a live cluster — only extending the dataset with new episodes requires one.

## Findings, in one table

| Fault class | Classification F1 | Permutation p | Baseline lead time | ML lead time | Verdict |
|---|---|---|---|---|---|
| disk_pressure | 0.941 | < 0.01 | 0.0 s (8/8) | 98.6 s (mischaracterized — §3.2) | Significant but not early detection |
| broker_kill | 0.762 | 0.25 | N/A (recall 12.5%) | not computed | No validated signal |
| network_degradation | 0.118 | 1.00 | 0s×3, 65s×1 (4/8) | not computed | Cleanest negative result |
| executor_oom | 0.909 (N=15) | 0.500 | 64.9 s (48–83 s) | not computed | No validated signal |
| backpressure_cascade | not modeled | — | excluded | — | No comparison possible |

Full narrative, mechanism-verification trace, and figures: `docs/paper_full_draft.md`.

## Status

This is an active M.Tech research project. The infrastructure, fault-injection
campaign, dataset, baseline evaluation, and ML modeling are complete and verified. The
paper (`docs/paper_full_draft.md`, ported to `IEEE-Template/kspfail.tex`) is
content-complete and pending compilation/submission. `docs/research_context.md` is the
full build log — the authoritative record of every methodological decision and why it
was made.

## License

Not yet decided. Do not assume a license from this file's absence of one; see
[`results/DATASET.md`](results/DATASET.md#license) for the current status.
