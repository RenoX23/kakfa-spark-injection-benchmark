<!--
Paper tables, assembled 2026-07-19. Every number traces to a committed source, cited
per table under "Source:". These are the tables the manuscript needs beyond the main
results table (which already lives in paper_draft.md §3.1 as "Table 1").

Final table NUMBERS are provisional: only the Results section is drafted so far, so the
Related-Work and Methodology tables can't be given fixed numbers until those sections
exist. They are grouped here by the section they belong to, in IMRAD reading order.

Honesty carries over from the sources: the results are null-heavy. Only disk_pressure
clears significance, and §3.2 shows even that is a mischaracterized (era-level) signal.
No table here dresses that up.
-->

# KSPFail — Paper Tables

Tables for the manuscript, grouped by section. The main results table already exists as
**Table 1** in [`paper_draft.md`](paper_draft.md) §3.1 and is not duplicated here — it is
listed in the Results block below for placement only.

---

## Related Work

### Positioning grid — where prior work sits vs. this work

The gap argument as a feature grid: no prior row satisfies all six criteria; only this work does.

**Source:** [`literature/LITERATURE_MATRIX.md`](../literature/LITERATURE_MATRIX.md) (each mark traceable to the cited paper).

| Work | Controlled fault injection | ML failure prediction | Lead-time evaluation | Static-threshold baseline | Kafka + Spark Str. Streaming | Open labeled dataset |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| Wu et al. (2021), DRAM | — | ✓ | — | — | — | ◐ |
| Harrison et al. (2018) | — | ✓ | — | — | — | — |
| Lin et al. (2018), MING | — | ✓ | — | — | — | — |
| Alharthi et al. (2023), *Time Machine* | — | ✓ | ✓ | — | — | ◐ |
| Basiri et al. (2016), *Chaos Engineering* | ✓ | — | — | — | — | — |
| Chen et al. (2025), K8s failure injection | ✓ | — | — | — | — | ◐ |
| Vogel et al. (2024), DEBS'24 | ✓ | — | — | — | ✓ | ✓ |
| Al-Sayeh et al. (2020), gray-box Spark | — | ◐ (runtime, not failure) | — | — | ◐ (Spark) | — |
| **This work (KSPFail)** | **✓** | **✓** | **✓** | **✓** | **✓** | **✓** |

✓ = fully addressed; ◐ = partial; — = not addressed. Surveys (Notaro 2021, Zhang 2024,
Salfner 2010, Natella 2016) are omitted from the grid — they catalog the field rather than
occupy a point in it — but each is cited in the related-work prose. The closest neighbours a
reviewer will press on are **Vogel/DEBS'24** (same substrate, measures recovery not
prediction) and **Time Machine** (predicts lead time, but HPC logs, no fault injection, no
threshold baseline); both need a dedicated paragraph, not just this row.

---

## Methodology

### Fault taxonomy — the five injected fault classes

**Source:** [`docs/baseline_thresholds.md`](baseline_thresholds.md) §1–5; injection scripts `fault_injection/*.py`.

| Fault class | Injection mechanism | Target component | Prometheus signal | Static-threshold rule | In modeling set? |
|---|---|---|---|---|:---:|
| broker_kill | Delete the sole Kafka broker pod (outage until reschedule) | Kafka broker | `up{job="kafka-broker-jmx"} == 0` | binary target-down | yes |
| executor_oom | Gradual memory ramp until the OOM kill | Spark executor pod | `container_memory_working_set_bytes{container="spark-kubernetes-executor"}` | `> 900 MB` (≈75% of the 1152Mi cgroup limit) | yes |
| disk_pressure | One-shot 3 GB fill of `/var` | Kind node filesystem | `node_filesystem_avail_bytes{mountpoint="/var"}` | Δ-drop `> 1.5 GB` (severity `> 2.90 GB`) | yes |
| network_degradation | `tc netem` delay + jitter + 10% loss, 90 s | Broker network path | `scrape_duration_seconds{job="kafka-broker-jmx"}` | `> 1.5 s` (severity `> 3.85 s`) | yes |
| backpressure_cascade | Burst load → Spark consumer lag | Spark Structured Streaming | *no independently-scraped metric exists* | N/A | **no** — excluded (only lag source is the driver-log field the label itself uses; a detector on it would be circular) |

Thresholds are calibrated on each class's own observed baseline-vs-fault Prometheus values
(same-corpus, no held-out split — a disclosed baseline-detector limitation, `baseline_thresholds.md` "Scope limitation").

### Dataset summary — episodes, exclusions, and modeling windows

**Source:** [`results/DATASET.md`](../results/DATASET.md); `results/campaign-n8/_discarded/*/README.md`; `results/ml-first-pass/extracted_windows.csv` (102 rows, verified).

| Fault class | Active episodes | Discarded | Discard reason (summary) | Ground-truth confirmation field | Window rows |
|---|:---:|:---:|---|---|:---:|
| broker_kill | 8 | 0 | — | `target_recovered_utc` | 26 |
| executor_oom | 15 | 10 | 8 superseded instant-onset design; 2 config-fix risk window | `oomkilled_confirmed_utc` | 24$^{a}$ |
| disk_pressure | 8 | 1 | drop never confirmed within the injection script's polling window | `drop_confirmed_utc` | 26 |
| network_degradation | 8 | 8 | timeout-censored reps from a since-fixed harness issue | `degraded_health_detected_utc` | 26 |
| backpressure_cascade | 8 | 1 | provenance gap (Spark config-bug risk window) | `caught_up_utc` | 0$^{b}$ |
| **Total** | **47** | **20** | every exclusion has a per-episode recorded reason | — | **102** |

$^{a}$ Of 15 active executor_oom episodes, 11 yielded ≥1 usable classification window (4 lost
to Prometheus scrape-timing misses on the pod-scoped memory metric). $^{b}$ backpressure_cascade
is collected with real ground truth but absent from the modeling table — see taxonomy note.

---

## Results

### Table 1 (existing) — main classification + lead-time outcomes

Already drafted in [`paper_draft.md`](paper_draft.md) §3.1. Reports, per class: LOO-CV F1 on
the class-specific corrected feature set, rank-based permutation *p* (100 shuffles), baseline
lead time, ML lead time, and verdict. **Not reproduced here** to avoid a divergent copy.
Headline: only disk_pressure clears significance (F1 = 0.941, *p* < 0.01), and §3.2 shows it
is an era-level absolute-value discriminator, not early fault detection.

### Baseline static-threshold detector performance

The static-threshold detector that reproduces current alerting practice — the comparison target for RO3/RQ4.

**Source:** [`results/baseline-threshold-evidence/evaluation_output.json`](../results/baseline-threshold-evidence/evaluation_output.json) (via `baseline_thresholds.md` "Full Evaluation Results").

| Fault class | TP | FN | FP | Recall | Precision | F1 | Mean baseline lead time | Target event |
|---|:---:|:---:|:---:|:---:|:---:|:---:|---|---|
| broker_kill | 1 | 7 | 0 | 12.5% | 100% | 0.22 | N/A$^{c}$ | crash / outage |
| executor_oom | 8 | 0 | 0 | 100% | 100% | 1.00 | 64.9 s (range 48–83 s) | crash / outage |
| disk_pressure | 8 | 0 | 0 | 100% | 100% | 1.00 | 0.0 s (8/8)$^{d}$ | severity threshold |
| network_degradation | 8 | 0 | 0 | 100% | 100% | 1.00 | 0 s ×3, 65 s ×1 (only 4/8 reach severity)$^{d}$ | severity threshold |
| backpressure_cascade | — | — | — | — | — | — | — | excluded |

100% recall on executor_oom / disk_pressure / network_degradation reflects same-corpus
threshold calibration (well-separated thresholds), **not** out-of-sample generalization —
disclosed, not hidden. $^{c}$ broker_kill's 57.0 s figure is residual outage *after* detection,
not a lead time (a binary up/down signal has no distinct later crash event); reported N/A as a
lead time, per §3.1 footnote b. $^{d}$ disk_pressure/network_degradation severity lead time is
≈0 s because the real telemetry jumps from baseline to full fault magnitude in a single 60 s
scrape step — a structural characteristic, not a tuning failure (§3.2–3.3).

### Multi-model robustness — RF vs XGBoost vs LightGBM

Shows the null results are **robust to model choice**: the three tree families agree per class.
The signal (or its absence) is in the data, not the estimator.

**Source:** [`results/ml-first-pass/multi_model_nested_tuning.json`](../results/ml-first-pass/multi_model_nested_tuning.json). F1 from nested-tuned LOO-CV on the raw 6-feature windows (`n_windows` = 26, `n_groups` = 18 for the N = 8 classes).

| Fault class (config) | RF F1 | XGBoost F1 | LightGBM F1 | Best model | Permutation *p* |
|---|:---:|:---:|:---:|:---:|:---:|
| broker_kill (raw)$^{e}$ | 0.667 | 0.667 | 0.667 | RF | 0.26$^{e}$ |
| disk_pressure (raw)$^{f}$ | 1.000 | 0.933 | 1.000 | RF | < 0.01 |
| network_degradation (raw) | 0.429 | 0.385 | 0.370 | RF | 0.87 |
| executor_oom (N = 8, 5 ep) | 0.800 | 0.000$^{g}$ | 0.800 | RF | 0.48 |
| executor_oom (clean 3 ep) | 0.667 | 0.000$^{g}$ | 0.667 | RF | 0.56 |

$^{e}$ broker_kill's raw result is driven by an `n_samples` leak (100% feature importance);
de-confounded (no `n_samples`) RF F1 = 0.762, *p* = 0.26 — the number reported in Table 1.
$^{f}$ disk_pressure raw-feature F1 = 1.000 here vs Table 1's **delta**-feature F1 = 0.941;
different experiments. Read §3.2 first — the significant result is an era-level discriminator,
not early detection. $^{g}$ XGBoost degenerates to all-negative predictions on the small
executor_oom folds (F1 = 0). The N = 15 top-up result (RF F1 = 0.909, *p* = 0.500) is reported
separately in Table 1 / §3.4 and post-dates this comparison.

---

## Optional / supplementary (available, not yet built into a table)

- **Window/horizon sensitivity** (`results/ml-first-pass/window_horizon_sweep.json`): F1 across
  4 window/horizon grids (10 s/[5,10] … 30 s/[15,30]) per class, bounded by each class's real
  min inter-episode gap. Supports a robustness/appendix table if a reviewer questions the
  window choice. Say the word and I'll build it.
- **SHAP attribution** already has a figure (Figure 1, `disk_pressure_shap_summary.png`); the
  underlying per-feature percentages (delta_mean 27.2%, delta_min 25.1%, delta_last 23.0%,
  delta_max 22.1%; 97.4% magnitude-feature share) are in §3.2 prose and could become a small table.
