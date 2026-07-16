# KSPFail Dataset — Card

A labeled dataset of real, physically-injected faults on a live Kafka + Spark
Structured Streaming pipeline, with ground-truth onset timestamps recorded by the
injection tooling itself (not inferred after the fact) and paired Prometheus telemetry
for pre-failure lead-time modeling. Collected 2026-07-11 through 2026-07-13 against a
single-node Kind cluster (Strimzi Kafka + Spark 4.1.2 Structured Streaming +
Prometheus, `infra/`).

This file is the standalone entry point — read this first, not
`docs/research_context.md`'s full build log, if you just want to use the data.

## Scale

| Fault class | Active (used) | Discarded | Discard reason |
|---|---|---|---|
| broker_kill | 8 | 0 | — |
| disk_pressure | 8 | 1 | `drop_confirmed_utc` never set within the injection script's polling window (real fault, timing-sensitive detection) |
| network_degradation | 8 | 8 | Timeout-censored reps from a since-fixed harness issue, superseded by a clean full re-collection |
| backpressure_cascade | 8 | 1 | Provenance gap — rep window overlapped a since-fixed Spark config bug's risk period |
| executor_oom | 15 | 10 | 8 from the original instant-onset injection design (superseded by a gradual-ramp redesign), 2 from a config-fix risk window |

**47 active episodes, 20 discarded — every exclusion has a per-episode reason recorded**, not silently dropped. Full detail per discarded episode: `results/campaign-n8/_discarded/*/README.md` (5 subdirectories, one per discard category above).

Processed feature table (`results/ml-first-pass/extracted_windows.csv`): 102 labeled window rows across the 4 classes with a validated-or-tested classification signal (broker_kill, executor_oom, disk_pressure, network_degradation). backpressure_cascade is not in this table — see Scope note below.

## Schema

### Raw ground truth: `results/campaign-n8/<class>/<fault_class>_run<id>.json`

One file per real injection. Fields vary by class (each fault has a different detection mechanism), always including `run_id`, `injection_timestamp_utc` (when the fault was caused, recorded by the script — not observed after the fact), a class-specific confirmation timestamp, and `recovered` (bool).

| Class | Confirmation field (the "failure/recovery event") | Class-specific fields |
|---|---|---|
| broker_kill | `target_recovered_utc` | `target_unhealthy_detected_utc` |
| executor_oom | `oomkilled_confirmed_utc`, `new_executor_recovered_utc` | `driver_pod`, `baseline_restart_count` |
| disk_pressure | `drop_confirmed_utc`, `cleaned_up_utc` | `fill_size_gb`, `baseline_avail_bytes`, `post_fill_avail_bytes` |
| network_degradation | `degraded_health_detected_utc`, `target_recovered_utc` | `delay_ms`, `jitter_ms`, `loss_pct`, `duration_s`, `baseline_scrape_duration_seconds`, `peak_scrape_duration_seconds_during_fault` |
| backpressure_cascade | `caught_up_utc` | `burst_size`, `baseline_lag_seconds`, `peak_lag_seconds_observed` |

### Processed features: `results/ml-first-pass/extracted_windows.csv`

One row per (episode, horizon) window, columns: `fault_class`, `episode_id`, `label` (1=pre_failure, 0=normal), `horizon_s` (seconds before onset the window ends; null for normal-reference rows), `window_kind` (`pre_failure` / `normal` / `normal_reference`), `mean`, `std`, `min`, `max`, `last`, `n_samples` (raw Prometheus statistics over that window). `delta_*` features (episode-relative-baseline versions of `mean`/`min`/`max`/`last`) are computed on top of this table per-script, not stored here, since the baseline differs by use case (see `docs/research_context.md` Section 6.3).

### Label construction

- `pre_failure` windows: real telemetry in the horizon window(s) before each real episode's own recorded onset.
- `normal_reference` windows (broker_kill, disk_pressure, network_degradation only, 10 each): real telemetry sampled from a genuinely quiet period with no fault active, evenly strided across a 67-minute reference window — group identity is constructed (these don't correspond to a discrete real episode), but the measured values are real Prometheus scrapes, not fabricated.
- `normal` windows (executor_oom only): each real episode's own pre-injection settling period, since this class is pod-scoped and has no shared long-lived "normal" pod to draw a pooled reference from.

## Scope note: backpressure_cascade

Collected (8 active episodes, real ground truth) but **not present in the modeling dataset**. No independently-scraped Prometheus metric exists for Spark processing lag on this deployment — the only lag field available is the driver-log-parsed one the ground truth itself already uses, so any model built from it would be circular. Documented in `docs/baseline_thresholds.md` Section 3. The raw episodes remain in `results/campaign-n8/backpressure_cascade/` for anyone with a different telemetry source for this signal.

## Reproduction / extension

- Injection scripts: `fault_injection/*.py`, one per class, orchestrated by `fault_injection/campaign.py` (`--top-up-class <class> --top-up-target <n>` to extend any class).
- Infra: `infra/` (Kind, Strimzi Kafka, Spark submit manifests, Prometheus Helm values) — declarative, `kubectl apply -f`, not imperative setup.
- Feature extraction: `modeling/extract_and_train.py`.
- Requires a live cluster matching `infra/`'s topology to extend with new episodes; the existing 47+20 episodes' raw ground truth and extracted features are static and usable without one.

## License

**Not yet decided — do not assume a license from this file's absence of one.** No `LICENSE` file exists in this repository as of this dataset card's writing. Pick one explicitly before any external release or citation is invited (e.g. CC-BY-4.0 for the dataset, matching common practice for benchmark-paper data releases — a recommendation, not a decision made on your behalf here).

## Citation

Paper not yet published — no citation to give here. Update this section once a venue accepts the manuscript; do not add a placeholder citation with invented details before that's real.
