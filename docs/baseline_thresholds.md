# Baseline Static-Threshold Definitions (Weeks 6-7)

Per Section 6.4's non-negotiable requirement: a static-threshold detector reproducing real
alerting rules from current practice, evaluated on the *same* N=8/class fault-injection
dataset used for everything else. Every threshold below is grounded in this pipeline's own
observed telemetry (baseline vs. fault-window values, actually queried from Prometheus,
not assumed or borrowed from an industry rule of thumb) — same standard applied to every
other empirical claim in `docs/research_context.md` this session.

This is a from-scratch build. An earlier attempt at this file was redirected mid-session
into due diligence that found several of the originally-proposed thresholds structurally
unimplementable; that investigation never produced a committed file. This is the first one.

## 1. broker_kill

**Metric:** `up{job="kafka-broker-jmx"} == 0`
**Threshold:** binary target-down (no magnitude to tune — this *is* the standard
"target down" alert, one of the most common real Prometheus rules in production.)

**Why this metric and not something else:** the sole broker's JMX exporter becomes
entirely unreachable during the outage (pod deleted) — there is no *elevated value* on any
Kafka metric to threshold on, because nothing is being scraped from that pod at all during
the fault. `up` is the only signal that exists during the outage window, and it's also
this class's own ground-truth mechanism (`fault_injection/broker_kill.py`'s
`prom_target_health` check).

**Real limitation, verified empirically, not assumed:** broker_kill's outage durations
across the active N=8 dataset are short — 11-62s (`results/campaign-n8/broker_kill/`,
`target_recovered_utc` minus `injection_timestamp_utc`). The `kafka-broker-jmx` scrape job
has no per-job `scrape_interval` override, so it inherits the global 60s interval (confirmed
against live Prometheus config, `/api/v1/status/config`, not the committed YAML alone).
Queried the actual recorded `up` values across all 8 reps' outage windows directly:

| rep | outage duration | scrape landed inside the outage window? |
|---|---|---|
| campaign1 | 55s | No |
| campaign2 | 14s | No |
| campaign3 | 62s | **Yes** |
| campaign4 | 13s | No |
| campaign5 | 59s | No |
| campaign6 | 35s | No |
| campaign7 | 11s | No |
| campaign8 | 31s | No |

**Expected recall: ~1/8 (12.5%).** This is not a threshold-value problem — no numeric
choice fixes it, since the signal is binary. It's a genuine structural finding: at this
pipeline's real 60s scrape cadence, a static "target down" alert would miss most short
broker outages entirely. Unlike executor_oom's instant-onset design, this isn't something
to redesign — the outage duration is real Kubernetes pod-reschedule time, not a synthetic
parameter under this project's control. Reported as-is: this result is expected to directly
motivate RQ4 (does ML-based prediction outperform static-threshold alerting), not a
tuning failure to route around.

## 2. executor_oom

**Metric:** `container_memory_working_set_bytes{container="spark-kubernetes-executor"}`
(cAdvisor container-level metric — reflects the raw injected allocation directly, unlike
Spark's own JVM-level `metrics_executor_memoryUsed_bytes`, ruled out earlier this session).

**Threshold:** `> 900 MB` (≈75% of the 1152Mi / 1208MB decimal cgroup limit — also matches
how real Kubernetes memory-pressure alerts are typically framed, `working_set / limit >
0.75`, not an arbitrary number).

**Grounded in real observed values across the active N=8 ramp-design dataset**
(`results/campaign-n8/executor_oom/`, `ramptest3`-`ramptest10`), queried directly from
Prometheus per-pod, per-rep (not averaged across unrelated pods):

- Organic/settled baseline (before injection, i.e. normal executor memory with no fault
  active) ranges **395.7-525.2 MB** across the 8 reps — varies rep-to-rep because each
  executor is a fresh pod scheduled after the previous rep's kill, and some reps' 90s
  pre-injection window catches the replacement still warming up. 900MB sits **375MB+ above
  the highest observed settled baseline** — no false-fire risk from normal variation.
- Real ramp sequences (all 8 reps show a genuine 4-sample rising trend, not interpolation),
  e.g. ramptest6: 395.7 → 642.0 → 822.5 → 1074.4 MB before the kill.
- **All 8 of 8 reps cross 900MB before their own OOM kill** — 100% recall on this dataset —
  with real, verified lead time (query timestamp of the crossing vs. that rep's own
  `oomkilled_confirmed_utc`), not a last-scrape-before-kill trigger:

| rep | lead time (threshold crossing → OOM) |
|---|---|
| ramptest3 | 59s |
| ramptest4 | 65s |
| ramptest5 | 83s |
| ramptest6 | 48s |
| ramptest7 | 64s |
| ramptest8 | 65s |
| ramptest9 | 77s |
| ramptest10 | 58s |

Mean lead time ≈ 65s, range 48-83s. This is the direct payoff of this session's ramp
redesign (`docs/research_context.md` Section 6.2) — the original instant-onset design had
zero precursor duration and could not have produced any of this.

## 3. backpressure_cascade — EXCLUDED from static-threshold comparison

**Finding, not a workaround:** no independently-scraped Prometheus metric reflects Spark
processing lag on this deployment. Checked the driver's raw metrics output directly (both
`/metrics/executors/prometheus/` and `/metrics/prometheus/`) — despite
`spark.sql.streaming.metricsEnabled=true` being set, zero streaming/trigger/inputRows/lag
keys are exposed on either endpoint. The only place lag exists at all is the driver-log-
parsed `ts` field this class's own ground truth already uses
(`fault_injection/backpressure_cascade.py`). Building a "threshold" on that field would be
circular — detector and label sharing a source, producing a meaningless, inflated-by-
construction result.

Compounding, not the primary reason: even if a metric existed, this class's fault durations
(14-19s, `results/campaign-n8/backpressure_cascade/`) are well under the 60s scrape
interval — the same coin-flip problem as broker_kill.

**Disposition:** excluded from the precision/recall/F1 comparison. Reported explicitly as
a real limitation of current Prometheus-based alerting practice on this pipeline — this
class cannot be monitored by a static threshold without new instrumentation, which would
contradict this class's own original design principle (ground truth reuses existing fields,
zero new instrumentation, see Section 6.2). Whether the Weeks 8-9 ML models can do better —
working from raw per-record fields rather than this one derived aggregate — is an open,
separate question for that phase, not assumed here either way.

## 4. disk_pressure

**Metric:** `node_filesystem_avail_bytes{mountpoint="/var"}`

**Threshold:** delta-based — **drop of more than 1.5GB from rolling baseline**, not a
percentage-of-total. A percentage threshold was one of the originally-proposed thresholds
found structurally broken in the earlier due-diligence: this pipeline's "disk" is the Kind
node's real host filesystem (~995GB, not a properly bounded/sized volume — Kind's
`local-path-provisioner` doesn't enforce PVC quotas, documented in
`docs/research_context.md` Section 6.2), so any fixed-size fault injection is a vanishingly
small percentage of total regardless of severity. A percentage rule would essentially never
fire. An absolute-delta rule is also standard real-world practice for exactly this reason
(alerting on rate-of-change rather than raw percentage on large/elastic volumes).

**Grounded in real values:**
- Every rep's injected drop is a consistent **3.22GB** (`results/campaign-n8/disk_pressure/`,
  `baseline_avail_bytes` minus `post_fill_avail_bytes` — consistent because the fill size
  is a controlled 3GB parameter, not organic variance).
- Real steady-state noise floor, queried over a genuine 68-minute non-fault window
  (`node_filesystem_avail_bytes` during the executor_oom ramp campaign, when no
  disk_pressure fault was active): **207MB max natural fluctuation.**
- 1.5GB sits **~7x above the natural noise ceiling** and comfortably below the observed
  3.22GB real fault drop — clean separation, no false-fire risk from normal filesystem
  activity, reliable detection of the actual injected fault.

Detection latency (`drop_confirmed_utc` minus `injection_timestamp_utc`) across the 8 active
reps ranges 11-59s (mean ~39s, stdev ~18s per the existing `integrity_checkpoint()` stats) —
this is a genuinely real, if noisy, lead-time signal, unlike broker_kill/backpressure_cascade's
structural problems, because the fault's *total* duration (72-121s) comfortably exceeds the
60s scrape interval even though detection within that window varies.

## 5. network_degradation

**Metric:** `scrape_duration_seconds{job="kafka-broker-jmx"}` — not target up/down (never
flips at any tested severity, a finding already documented in Section 6.2) and not the
raw injected delay/loss parameters (not observable telemetry).

**Threshold:** `> 1.5s`

**Grounded in real values across the active N=8 dataset**
(`results/campaign-n8/network_degradation/`, the corrected 90s-duration, 20s-scrape_timeout
re-run):

| rep | baseline | peak during fault | ratio |
|---|---|---|---|
| campaign1 | 0.783s | 5.698s | 7.3x |
| campaign2 | 0.742s | 2.638s | 3.6x |
| campaign3 | 0.615s | 2.267s | 3.7x |
| campaign4 | 0.675s | 3.061s | 4.5x |
| campaign5 | 0.751s | 3.194s | 4.3x |
| campaign6 | 0.694s | 3.374s | 4.9x |
| campaign7 | 0.789s | 2.804s | 3.6x |
| campaign8 | 0.668s | 2.644s | 4.0x |

Baseline never exceeds 0.789s across any rep; fault-window peak never drops below 2.267s
across any rep. 1.5s sits cleanly in that gap with real margin on both sides (≈0.71s above
the highest baseline, ≈0.77s below the lowest peak) — not a threshold chosen to flatter the
result, the gap itself is this wide in the actual data.

**Detection timing:** this class's 90s fault duration was deliberately chosen (Section 6.2)
specifically to guarantee — not just likely, mathematically guaranteed given a 90s window
against a 60s scrape period — at least one real scrape lands inside the fault window for
every rep. No coin-flip risk here, unlike broker_kill/backpressure_cascade.

## Summary

| class | threshold | expected recall (this dataset) | status |
|---|---|---|---|
| broker_kill | `up == 0` | ~12.5% (1/8) | real limitation, reported not routed around |
| executor_oom | `working_set > 900MB` | 100% (8/8), lead time 48-83s | clean, viable |
| backpressure_cascade | — | — | **excluded** — no independent signal exists |
| disk_pressure | `avail drop > 1.5GB` | pending full evaluation | clean, viable |
| network_degradation | `scrape_duration > 1.5s` | pending full evaluation | clean, viable |

Not yet run: the actual per-rep precision/recall/F1/false-positive-rate pass across all
reps (correctly-flagged vs. missed vs. false-fired-during-normal-operation), and the
full lead-time distribution for disk_pressure/network_degradation the way executor_oom's
is already shown above. Holding for review before running that, per standing instruction:
don't evaluate before the threshold definitions themselves are checked.
