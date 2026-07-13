# Lead-Time Failure Prediction for Kafka–Spark Structured Streaming Pipelines Using Observability Telemetry: A Fault-Injection Benchmark

**Document type:** M.Tech Research Proposal / Project Synopsis (working document)
**Domain:** Applied Machine Learning — AIOps, Data Engineering, Distributed Systems Reliability
**Student:** [To be filled]
**Guide:** [To be filled]
**Department / Institution:** [To be filled]
**Date:** [To be filled]

---

## How to use this document

This is a working reference, not a finished paper. Every claim is either (a) traceable to a source found and checked in this research pass, or (b) explicitly flagged as an assumption you still need to test on your own cluster. Don't hand this to a guide as-is — fill in the front matter, and don't cite anything in Section 12 without pulling exact venue/author details yourself where flagged.

**Before writing a line of code beyond what already exists**, re-run a saturation check: search `"Kafka" "Spark Structured Streaming" failure prediction benchmark 2026` and `AIOps streaming pipeline fault injection lead time 2026`. This space (AIOps failure prediction) is active and growing — see Section 3. If someone has closed this exact intersection by the time you start building the labeled dataset, pivot the framing, don't abandon the infrastructure.

---

## Abstract

Kafka–Spark Structured Streaming pipelines are core infrastructure in modern data platforms, and their failures (broker crashes, executor OOM kills, consumer-lag cascades, backpressure collapse) cause SLA breaches and data-freshness violations. Current industry practice for catching these failures is static-threshold alerting on Prometheus/JMX metrics (consumer lag, under-replicated partitions, GC pauses) — a reactive approach that fires *after* degradation is already visible. Separately, the AIOps research literature has established that ML-based failure prediction using system telemetry outperforms static thresholds in general distributed-systems contexts (Notaro, Cardoso & Gerndt, 2021), and empirical multi-classifier benchmarking has been done for hardware failure domains such as DRAM (Wu et al., 2021) and generic batch-cluster job failures (Ahmed & Fisher, 2018). No located work applies this benchmarking discipline — controlled fault injection, multi-model comparison, and **lead-time** evaluation — specifically to the Kafka–Spark Structured Streaming pipeline layer, as distinct from (a) anomaly detection on the data flowing *through* the pipeline, and (b) Spark job runtime/performance prediction, both of which are separately well-studied. This project closes that gap: it builds a reproducible fault-injection benchmark on a Kafka + Spark Structured Streaming pipeline instrumented with Prometheus JMX scraping, constructs a labeled pre-failure telemetry dataset across multiple fault classes, and benchmarks supervised ML models against the field's actual current practice (static thresholds) using lead time — not just classification accuracy — as the headline evaluation metric.

---

## 1. Introduction and Motivation

Streaming data infrastructure (Kafka for ingestion, Spark Structured Streaming for processing) is now standard in data engineering stacks, and its reliability is operationally critical: a stalled consumer group or a crashed broker silently degrades downstream freshness long before a human notices a dashboard turn red. The dominant operational practice today is threshold-based alerting — fire an alert when consumer lag exceeds N, or when under-replicated partitions exceed zero. This is reactive by construction: the threshold is crossed only once degradation is already underway.

The AIOps research community has spent roughly a decade establishing that ML-based failure prediction, trained on system telemetry (metrics, logs, traces), can catch degradation earlier than static rules across general IT infrastructure (Notaro, Cardoso & Gerndt, 2021). This has been demonstrated concretely in adjacent domains — DRAM failure prediction at data-center scale (Wu et al., 2021), node failure prediction in cloud platforms, and batch-cluster job failure prediction (Ahmed & Fisher, 2018) — but these are hardware- or generic-cluster-focused. The specific combination of **Kafka + Spark Structured Streaming as the object of failure prediction** (not the data flowing through it, and not job runtime/cost prediction, both of which are separately studied) has not been benchmarked with the same rigor: controlled fault injection to generate ground truth, multi-model comparison, and evaluation against what practitioners actually use today (threshold alerts).

## 2. Problem Statement

Given telemetry collected from a Kafka + Spark Structured Streaming pipeline (broker JMX metrics, Spark executor/streaming-query metrics, node-level resource metrics), can supervised ML models predict pipeline failure *before* it manifests as a threshold breach, with a quantifiable lead time — and how much earlier does this catch failures compared to the static-threshold alerting that constitutes current practice?

## 3. Literature Review and Positioning

| Source | What it established | What it does NOT do |
|---|---|---|
| Notaro, P., Cardoso, J., Gerndt, M. *A Survey of AIOps Methods for Failure Management.* ACM Trans. Intell. Syst. Technol. 12(6), Article 81 (2021). DOI: 10.1145/3483424 | Canonical AIOps survey; establishes fault-injection as the standard methodology for generating ground truth in failure-management research; catalogs sliding-window ML approaches for imminent-failure prediction | General survey, no streaming-pipeline-specific study; no Kafka/Spark benchmark |
| Wu, Z., Xu, H., Pang, G., Yu, F., Wang, Y., Jian, S., Wang, Y. *DRAM Failure Prediction in AIOps: Empirical Evaluation, Challenges and Opportunities.* arXiv:2104.15052 (2021) | Empirical multi-classifier benchmark (7 classifiers + 3 anomaly detectors) for hardware failure prediction at data-center scale, using large-scale telemetry (PAKDD/Alibaba competition data) | Hardware (DRAM) domain, not software pipeline infrastructure; no lead-time metric reported |
| *A Feature Engineering Approach for Business Impact-Oriented Failure Detection in Distributed Instant Payment Systems.* arXiv:2510.21710 (2025) | Recent (2025) example of AIOps + explainable feature engineering applied to failure detection in a specific distributed-system domain (payments), showing the "domain-specific AIOps benchmark" pattern is an active, current research move | Domain is payments infrastructure, not streaming data pipelines; **verify author list before citing** — not confirmed in this pass |
| Ahmed, M., Fisher, D. et al. *Bioinformatics Computational Cluster Batch Task Profiling with Machine Learning for Failure Prediction.* arXiv:1812.09537 (2018) | Job-failure prediction on batch HPC clusters via Random-Forest feature selection at production scale (95.4% overall accuracy, 88% on failed-job class) | Batch HPC scheduling context, not streaming pipelines; no lead-time evaluation; **verify exact author list before citing** |
| Gray-box modeling methodology for runtime prediction of Apache Spark jobs. *Distributed and Parallel Databases* (Springer, 2020) | Established Spark job **runtime/performance** prediction as a distinct, well-studied problem from failure prediction | Predicts execution time and resource needs, not failure — useful as a contrast to sharpen your framing, not a competitor to your gap |
| Industry practice (Kafka monitoring vendor content: Acceldata, Conduktor, IBM, AutoMQ, 2025–2026; XGBoost-based Kafka lag-prediction patent, US) | Confirms strong practitioner demand for predictive (vs. reactive) Kafka/Spark monitoring; confirms XGBoost/tree-based models are the default practitioner choice for this feature type | Patents and vendor blog posts are not peer-reviewed benchmarks; none report controlled fault injection, multi-model comparison, or lead-time metrics |
| [A Comprehensive Benchmarking Analysis of Fault Recovery in Stream Processing Frameworks](https://arxiv.org/abs/2404.06203), ACM DEBS 2024 (also arXiv:2404.06203) | Closest located adjacent work, found in the 2026-07-08 saturation re-check: chaos-engineering-inspired fault injection across Flink, Kafka Streams, and Spark Structured Streaming, comparing **recovery performance, stability, and recovery time** in a cloud-native (Kubernetes) environment | Measures post-hoc recovery speed, not predictive ML; no lead-time-to-failure metric; no static-threshold-vs-ML comparison; no labeled pre-failure dataset; compares three *competing frameworks* against each other rather than treating a fixed Kafka+Spark pipeline as the object of prediction. Confirmed via direct abstract fetch, not assumed from title. |

**Positioning statement:** The general principle — ML-based failure prediction beats static thresholds, validated via fault injection — is well established in AIOps (Notaro et al., 2021) and has been executed rigorously in adjacent domains (DRAM, batch clusters). Industry demand for the Kafka/Spark-specific version is clearly present (vendor tooling, a patent), but no located academic work runs the full rigorous benchmark — fault injection, multi-model comparison, lead-time evaluation, head-to-head against threshold alerting — specifically on the Kafka + Spark Structured Streaming pipeline layer. This is the gap this project fills.

## 4. Research Objectives

- **RO1**: Build a reproducible fault-injection benchmark on a Kafka + Spark Structured Streaming pipeline instrumented with Prometheus JMX scraping (extending existing Kind-cluster infrastructure).
- **RO2**: Construct a labeled pre-failure telemetry dataset across multiple fault classes via controlled, repeated fault injection with recorded ground-truth failure-onset timestamps.
- **RO3**: Benchmark supervised ML models (tree-based: Random Forest, XGBoost/LightGBM; optionally a simple temporal model) against a static-threshold baseline that reproduces current real-world alerting practice.
- **RO4**: Define and report a **lead-time metric** — how much earlier than actual failure onset a true-positive alert fires — alongside standard classification metrics, and false-positive rate under normal operation.
- **RO5**: Apply feature-importance / SHAP analysis to identify which telemetry signals drive early prediction per fault class, producing actionable operator-facing insight.

## 5. Research Questions

1. Can supervised ML models trained on windowed pre-failure telemetry predict pipeline failure onset with a meaningfully positive lead time?
2. Which telemetry signals (consumer lag, under-replicated partitions, executor GC time, backpressure indicators, node resource metrics) are most predictive, and does this vary by fault class?
3. How does prediction quality (precision, recall, lead time) vary across fault types (broker kill, executor OOM, backpressure cascade, disk pressure, network degradation)?
4. Does ML-based prediction outperform static-threshold alerting — the field's actual current practice — on the same fault-injection dataset, and by how much?

## 6. Proposed Methodology

### 6.1 Infrastructure (build from zero — verified, not assumed)
**Correction:** an earlier version of this document assumed prior scaffolding was reusable and estimated a shorter timeline on that basis. That assumption was checked directly (`kind get clusters`, `docker ps -a`) and found false — no Kind cluster, Kafka, or Spark infrastructure exists on this machine. Full infrastructure cost is paid from zero: Kind (Kubernetes-in-Docker) cluster running a Kafka broker, a Spark Structured Streaming job consuming from it, and Prometheus scraping Kafka's JMX exporter plus Spark's metrics endpoint and node-level resource metrics — all built in Phase 0 below, not assumed pre-existing.

### 6.2 Fault Taxonomy and Injection

**LOCKED (2026-07-08, Weeks 2-3 gate):** 5 fault classes, all confirmed feasible on the current
single-node Kind + single-broker Kafka topology built in Phase 0. **All five now have working,
verified injection tooling as of 2026-07-10** (`fault_injection/`) -- Weeks 2-3's deliverable
("scripts working end-to-end for at least one fault class") is done for all five, not just one:
- Broker kill / restart (pod delete) — proven end-to-end, `fault_injection/broker_kill.py`
- Executor OOM-kill (memory pressure induction on the Spark executor) — `fault_injection/executor_oom.py`
- Backpressure cascade (burst producer past consumer capacity) — `fault_injection/backpressure_cascade.py`
- Disk-pressure on broker (bounded fill of the broker's real data volume) — `fault_injection/disk_pressure.py`
- Network degradation (latency/packet-loss via `tc netem`, pod/interface-level — feasible single-node, unlike a true network *partition*) — `fault_injection/network_degradation.py`

**Dropped, documented as an accepted limitation, not forced:** Partition leader churn (forced
leader re-election). Requires a multi-broker Kafka cluster to have any leader to re-elect — the
Phase 0 topology is a single combined controller+broker node by design (`infra/kafka/kafka-single-broker.yaml`,
`replicas: 1`), and it stays that way rather than scaling up mid-campaign to chase one fault class,
consistent with Section 11's own guidance to prioritize fault types Kind can genuinely emulate over
forcing ones it can't. If time permits after the 5 locked classes are done, this can be revisited as
stretch scope — not blocking.

**Tooling, LOCKED:** manual scripts (`kubectl` for pod-level faults, `tc netem` for network
degradation), not Chaos Mesh. This is Section 11's own pre-authorized fallback, adopted as the first
choice rather than after a Chaos Mesh setup failure: the Phase 0 pilot already proved the manual
approach end-to-end with strong evidence quality, it's trivially scriptable for the N≥15-20 repeated
runs needed per class, and it avoids adding an operator/CRD/webhook layer on a node already running
Kafka+Spark+Prometheus+node-exporter+producer on a 5-CPU/10GB budget.

Each fault repeated N times (target N≥15–20 per class for a usable sample) at randomized injection points during steady-state load, to avoid the model learning injection-schedule artifacts instead of genuine pre-failure signal.

**Reusable injection tooling built and verified (2026-07-08):** `fault_injection/common.py` +
`fault_injection/broker_kill.py`. Not a one-off script -- `common.py` provides shared plumbing
(dynamic port-forwarding, Prometheus health/query helpers, ground-truth JSON recording) that every
locked fault class's script reuses. Verified by running the broker-kill script three times
end-to-end against the live cluster (`results/fault-runs/`), not just written and assumed correct.

**A real bug was found and fixed in the process, not glossed over:** the first two script runs
(`broker_kill_pod_delete_runtest1.json`, `runtest2.json`) exposed a genuine cascading-failure chain
that manual testing in Phase 0 hadn't surfaced (Phase 0's single pilot fault didn't repeat the
injection, so it never hit this): Kafka's broker storage was `type: ephemeral`, so the pod-delete
fault wiped all topic data and committed offsets on every restart. Spark's Kafka source correctly
detected this as data loss and hard-failed the whole streaming query
(`KafkaIllegalStateException: Some data may have been lost...`, `failOnDataLoss=true` is the
correct default, not a bug in Spark). A second, independent bug surfaced during the same
investigation: Spark's own shutdown-cleanup bulk-deletes leftover resources by label selector,
which needs the `deletecollection` RBAC verb -- `infra/spark/rbac.yaml` only granted `delete`,
a distinct verb in Kubernetes RBAC, causing a cascade of additional cleanup errors on every app exit.

**Fix, and proof the fix works:** switched Kafka broker storage to `persistent-claim` (5Gi PVC --
`infra/kafka/kafka-single-broker.yaml`), which also matches real production Kafka (no real
deployment runs on ephemeral storage). Added `deletecollection` to the Spark RBAC role. Then re-ran
broker-kill a third time (`runtest3-persistent.json`) against the fixed topology and confirmed via
driver logs (`results/weeks2-3-tooling/spark_survives_broker_kill_batches.txt`) that the Structured
Streaming query kept processing real batches with non-zero windowed event counts continuously
through the fault window -- no crash, no restart. This is the standard this project holds itself to:
a fix isn't "done" until it's demonstrated working under the same conditions that exposed the bug,
not just applied and assumed correct.

A secondary, unrelated issue also surfaced and was fixed during this recovery: the console producer
(`infra/kafka/producer-loadgen.yaml`) uses idempotent-producer semantics by default, which got into
a confused epoch state across the broker recreation (`OUT_OF_ORDER_SEQUENCE_NUMBER` /
`InvalidProducerEpochException`). Fixed by disabling idempotence for this synthetic load generator
(`enable.idempotence=false`) -- exactly-once guarantees aren't a requirement for background load
generation, and idempotence was adding failure surface with no research-relevant benefit.

**Second fault class built and verified (2026-07-08): `fault_injection/executor_oom.py`.**
Induction: `kubectl exec` into the running Spark executor container, allocate memory well past its
1152Mi resource limit, forcing the kernel OOM-killer. This one took four iterations to get right --
documented in full because each iteration is a real, distinct finding, not noise to hide:

1. First run crashed the *script itself* -- `find_executor_pod` raised `subprocess.CalledProcessError`
   (kubectl errors, doesn't return empty, when a jsonpath indexes an empty pod list), not the
   `RuntimeError` the calling code was catching. Fixed by making that kubectl call non-raising and
   checking its output directly.
2. Second run (`runtest2-stateless.json`) revealed a **second, more fundamental bug**, this one in the
   pilot Spark script itself, not the injection tooling: the original script did a stateful windowed
   aggregation (`groupBy(window(...))`), and Structured Streaming's state store for that lives on the
   executor's local ephemeral disk. Killing the executor destroyed its state-store delta files, and
   the replacement executor had no way to reconstruct them -- the query crashed with
   `CANNOT_READ_DELTA_FILE_NOT_EXISTS`. Fixed by removing the aggregation entirely
   (`infra/spark/structured_streaming_kafka_read.py`, `infra/spark/configmap-script.yaml`): the
   stateful windowing was never required to prove Kafka-to-Spark connectivity (Phase 0's actual goal),
   it was an unnecessary addition that introduced a whole failure class this pilot didn't need to
   solve. Real windowing/state-persistence is a deliberate decision for the RO2 feature-engineering
   pipeline later, not inherited from this connectivity pilot.
3. Third run (`runtest3-driverlog.json`) still showed `oomkilled=False` even after switching detection
   to check the driver's log instead of the executor pod's own status (the pod-status approach raced
   against Spark's driver actively deleting the dead executor pod object almost immediately -- a
   genuine, useful finding in itself: **executor identity churns and disappears fast**, similar in
   spirit to broker_kill's instance-IP churn finding, so ground truth has to come from a durable
   source, not the ephemeral pod object). The driver-log switch was the right call, but had its own
   bug: the regex spanned `.*` across what are actually two separate log lines
   (`"Lost executor N ..."` then `"...exit code 137..."` on the next line), and `.` doesn't match
   newlines by default in Python regex -- so it silently never matched.
4. Fourth run (`runtest4-fixed.json`), all three fixes applied: `oomkilled=True` confirmed 5s after
   injection, new executor recovered 6s after injection, streaming job kept processing real data
   throughout with 0 driver restarts. Evidence: `results/weeks2-3-tooling/executor_oom_driver_log_excerpt.txt`.

**executor_oom injection redesigned from instant step-function to gradual ramp, 2026-07-11 (Weeks
4-5 campaign).** The instant-onset design above was ground-truth-correct but undetectable by any
independent telemetry signal: a single `bytearray` allocation blowing past the 1152Mi limit in one
shot has zero precursor duration by design, so against Prometheus's 60s scrape interval every scraped
metric (`container_memory_working_set_bytes` included) shows one before/after jump with no buildup
phase to sample. Ruled out faster detection first, not just jumped to redesign: lowering
`spark-driver`/cadvisor scrape intervals was ruled too risky/wrong-layer; re-enabling
kube-state-metrics at a 5s scrape and checking `kube_pod_container_status_restarts_total` came back
negative on its own merits, not just slow -- Spark's executor-loss handling replaces the whole pod
object (new name/UID) rather than restarting a container in place, so that metric structurally cannot
observe this fault class regardless of scrape interval. Redesigned the injection itself instead:
`fault_injection/executor_oom.py` now allocates in 25MB chunks (36 chunks, 7.5s sleep between),
calibrated against a measured baseline (~484MB) and gap-to-limit (~724MB) to target a 180-240s
injection-to-OOM window -- long enough to guarantee 3-4 real (not interpolated) Prometheus samples
during the rise. Validated across the final 8-rep dataset (`ramptest3`-`ramptest10`): durations
209-233s (mean 226.1s, stdev 7.5s), all with a genuine multi-point rising
`container_memory_working_set_bytes` trend. Old instant-onset reps kept as evidence, not deleted --
`results/campaign-n8/_discarded/executor_oom-instant-onset/README.md`.

**Third, fourth, and fifth fault classes built and verified (2026-07-10, after a 2-day gap in the
session -- see the environment-recovery note below): `fault_injection/backpressure_cascade.py`,
`disk_pressure.py`, `network_degradation.py`.** All five locked fault classes now have working,
evidenced tooling -- Weeks 2-3's deliverable is fully done, not just "at least one."

**Backpressure cascade.** Ground truth reuses the existing `ts` field already in every message
(no new instrumentation): burst-produce far more messages than Spark's 5s trigger interval can
absorb, then measure the gap between wall-clock now() and the `ts` of the most recent record Spark
has actually printed. Verified twice at different burst sizes, and the signal scales correctly with
severity -- exactly what a defensible methodology needs, not just "it fired once":
3000-message burst -> 17.1s peak lag (`runtest1.json`); 8000-message burst -> 24.0s peak lag
(`runtest2-bigger.json`). Confirmed the bursts genuinely landed and were processed (not just a lag
number floating free of evidence) by finding the burst's negative-seq marker rows in the driver log,
batched in oversized groups exactly as expected -- `results/weeks2-3-tooling/backpressure_burst_rows_excerpt.txt`.

**Disk-pressure on broker -- the injection mechanism changed mid-implementation, and the reason
matters.** Originally planned as a Kubernetes ephemeral-storage resource limit (fill past a small,
explicit limit, let kubelet's real eviction mechanism fire, entirely bounded and safe). Turned out not
to apply: ephemeral-storage limits govern a container's writable layer and un-sized `emptyDir`s, NOT
PersistentVolumeClaim-backed volumes -- and the broker's data directory is deliberately a PVC (the
Weeks 2-3 broker_kill fix). Switching the data volume back to `emptyDir` to make that limit apply
would have undone that fix. A second, separate discovery compounded this: Kind's default storage
provisioner (`local-path-provisioner`) doesn't enforce PVC size quotas at all -- `df` inside the
broker showed the real host disk (1007G total, 929G free at the time), not a bounded 5Gi volume, and
this machine runs other unrelated projects' containers, so filling toward real disk exhaustion to
trigger genuine Kubernetes disk-pressure eviction was never a safe option here regardless.
Reframed with the user's explicit sign-off: fill the broker's real (PVC-backed) data directory with a
small, bounded amount (3GB, trivial against 929GB free) and observe the drop via node-exporter's
`node_filesystem_avail_bytes` (already scraped by Prometheus since Phase 0). This doesn't trigger a
full Kubernetes eviction, but is arguably a better fit for what this dissertation actually measures --
gradual degradation telemetry (RO4's lead-time framing), not just binary crash/recovery, which
broker_kill and executor_oom already cover. Verified end-to-end (`runtest4-fixed.json`): baseline
996,570,152,960 bytes available -> 993,348,444,160 after a 3GB fill (a 3.22GB drop, matching
filesystem block overhead on top of the requested 3GB), detected within 5s, fully reclaimed and
confirmed within ~63s of cleanup. Two smaller bugs on the way: the mountpoint label in this
node-exporter's view isn't `"/"` (Kind's node container doesn't expose one) -- the large disk shows up
as `"/var"` instead, found by querying the metric's actual label values rather than guessing; and the
first cleanup-confirmation poll used too short a timeout (60s) against node-exporter's own scrape
interval, timing out even though `df` directly confirmed the space had already been reclaimed.

**Network degradation -- three real technical findings, not a clean first pass.** The Strimzi Kafka
image has no `tc`/iproute2 at all (`which tc`: "executable file not found"). Fixed with the standard
approach: an ephemeral debug container (`nicolaka/netshoot`) attached to the broker pod via
`--target`, sharing its network namespace, using Kubernetes's built-in `netadmin` debug profile to
grant CAP_NET_ADMIN to the debug container specifically -- the broker's own container and security
posture are untouched. Three findings surfaced building this:
1. A design bug caught before it could produce misleading results: the injection script's first draft
   called `kubectl debug` synchronously, which blocks for the fault's entire duration (its own `sleep`
   runs inside the debug session) -- meaning a synchronous call wouldn't return until *after* the fault
   already ended, so any poll for degradation starting afterward would always measure a healthy
   pipeline and silently prove nothing. Fixed by running the debug session as a background process and
   polling while it's still active.
2. Removal verification was unreliable when based on parsing the debug session's captured stdout for a
   `"NETEM_REMOVED"` marker -- a run came back with that check failing even though the Kubernetes-
   reported ephemeral container status showed a clean `exitCode: 0` for the same session. Switched to
   querying `ephemeralContainerStatuses` directly (the authoritative source) instead of a side-channel
   string match, which then needed a short poll of its own: the K8s API's ephemeral-container status
   lags a few seconds behind the container actually exiting.
3. **Binary target health (up/down) never flipped at any severity tested, including 500ms delay + 20%
   loss for 90s.** This is a genuine methodological finding, not a failed injection: this pipeline's
   Prometheus scrape is tolerant enough (generous default timeout) that network degradation at
   realistic severities doesn't cross the up/down threshold. `scrape_duration_seconds` is the metric
   that actually carries the signal -- confirmed clearly once fault duration exceeded Prometheus's own
   60s scrape interval (a 30s fault is a coin-flip on whether any scrape happens during the window at
   all -- `runtest2-mild.json` happened to catch one, 0.75s -> 2.27s; `runtest3-verified.json` didn't,
   same value before and after; `runtest4-longer.json` at 90s duration removed the coin-flip entirely,
   0.79s -> 6.65s, an unambiguous 8.5x spike). This directly informs the Weeks 4-5 campaign: network
   degradation fault durations must exceed the scrape interval, and `scrape_duration_seconds` belongs
   in the feature set for this fault class specifically -- target health alone would train a model that
   never learns to see this fault.

**network_degradation's original N=8 campaign run discarded wholesale and re-collected, 2026-07-11
(Weeks 4-5 campaign) -- a measurement-ceiling artifact, not genuine fault behavior.** Prometheus's
`kafka-broker-jmx` scrape job had no per-job `scrape_timeout` override, so it inherited the global
10s default. At this class's injection severity (500ms delay, 20% loss), 5 of the original 8 reps'
peak `scrape_duration_seconds` landed in a 9.8-10.01s band -- hard against that 10s ceiling, not what
an unbounded degraded-network distribution looks like. First misread as genuine bimodal behavior
before the ceiling was recognized. Fixed by adding a per-job `scrape_timeout` override (raised to
20s) in `infra/prometheus/values.yaml`, then re-running the full class (not just the affected reps --
the whole class was compromised, not individual outliers). Original 8 reps kept as evidence, not
deleted -- `results/campaign-n8/_discarded/network_degradation-timeout-censored/README.md`.

**Environment recovery, same session, after a 2-day gap in wall-clock time between turns.** The Kind
cluster's container survived (Docker/WSL2 restart policy held this time, unlike the earlier full wipe),
but with real drift: kubeconfig pointed at a stale host port (Kind reassigns a new one on container
restart unless pinned -- fixed with `kind export kubeconfig`), Spark's namespace was completely empty
(bare Pods, unlike Kafka's Strimzi-managed/PVC-backed pods, don't survive a node-level restart and
nothing recreates them), and the Prometheus Helm release had reverted to an earlier revision missing
the `spark-driver` scrape job and node-exporter -- both already correctly present in the committed
`infra/prometheus/values.yaml`, so the fix was a single `helm upgrade` reconciling the live release
back to the git-committed source of truth, not a rebuild. This is the infra-as-code discipline paying
for itself a second time this project.

**Weeks 4-5 campaign orchestrator built (`fault_injection/campaign.py`) and pilot-tested before
committing to the full N>=15-20 run -- and the pilot caught a serious, genuinely important bug.**
Reuses each fault class's `run()` function directly, sequential by class (not interleaved), randomized
steady-state gaps (45-90s) between repetitions per Section 6.2's own methodology requirement, manifest
written incrementally so an interrupted campaign loses at most the in-flight repetition.

A first N=2-per-class pilot (`results/campaign-pilot/`) ran broker_kill clean (2/2 ok, ~63-70s/rep),
then executor_oom and backpressure_cascade both failed immediately (4/4 errors) -- executor_oom with
"no running executor/driver pod found" (no Spark pod existed at all), backpressure_cascade with
"refusing to inject: baseline lag is Nones..." (a driver pod existed but the query wasn't processing,
so lag came back `None`). Different error strings, same root cause. Investigation found the actual
root cause was much more serious than a campaign-design issue: **the live Kafka broker's storage type
had silently reverted to `ephemeral`**
(confirmed via `kubectl get kafkanodepool single -o jsonpath='{.spec.storage}'`), even though the
committed `infra/kafka/kafka-single-broker.yaml` correctly specifies `persistent-claim` and had never
been wrong. The reverted CR's own `creationTimestamp` traced back to the *original* Phase-0 deployment
from before the persistent-storage fix was ever made -- live cluster state had drifted back to an
earlier point without the git-committed source of truth ever changing, the same class of problem as
the Prometheus Helm-release reversion found during the same session's environment recovery, just
undetected until a real `broker_kill` repetition exposed it by wiping the topic (offset dropped from
23920 to 33) and crashing the Spark query on the resulting data-loss exception. Fixed the same way as
the original Weeks 2-3 fix: delete and recreate the Kafka broker from the committed manifest, relaunch
Spark. A second pilot targeting just the two previously-failed classes (`results/campaign-pilot2/`)
then ran clean: 4/4 ok, executor_oom ~6.2-6.5s/rep, backpressure_cascade ~16.0-16.8s/rep.

This is a significant methodological lesson beyond just "another bug fixed": **being checked into git
is necessary but not sufficient for infrastructure correctness.** Live cluster state can silently drift
from the committed source of truth in ways that stay completely invisible until a specific operation
exercises the drifted part -- Kafka looked "Running" with genuine, growing offsets for over an hour of
subsequent work before this happened, giving no signal anything was wrong. Added a `preflight_check()`
to `campaign.py` that verifies live state (Kafka storage type, Kafka/Spark pod health) actually matches
what's committed *before* spending hours running a campaign on top of it, so this class of drift fails
fast with one clear error instead of silently burning through a chunk of the campaign's repetitions.
Real per-repetition timing from the validated pilots, used to size the full N=15-20 run: broker_kill
~134s (fault+gap), executor_oom ~74s, backpressure_cascade ~84s, disk_pressure ~159s, network_degradation
~166s -- full campaign at N=15/class is ~2.6 hours, N=20/class is ~3.4 hours.

### 6.3 Labeling
Sliding-window supervised framing, following the standard AIOps approach (Notaro et al., 2021): telemetry windows at multiple horizons before recorded failure onset are labeled "pre-failure"; windows during confirmed steady-state operation are labeled "normal." Window size and horizon are hyperparameters to sweep, not fixed a priori — report sensitivity, and the first empirical pass (2026-07-12) is exactly that kind of correction, not an exception to it.

**Horizons/window size corrected TWICE from the original illustrative example, both
times by measurement against real data, not by assumption — current final values:
[10s, 15s] horizons, 15s window.**

*Correction 1 (2026-07-12):* the original text here proposed t-30s/t-60s/t-120s
horizons with (implicitly, from the initial implementation) a 90s window — an a-priori,
textbook-style choice, not grounded in this campaign's own real spacing. Building
`modeling/extract_and_train.py` found that scheme needs ~210s of lookback room per
episode, but real inter-episode gaps across all 4 evaluated classes' campaigns are only
32-90s (measured directly from the ground-truth JSON timestamps, not assumed) — so the
original horizons collapsed almost every episode's "normal" window (7 of 8 skipped for
most classes on the first extraction attempt). Corrected to [15s, 30s] horizons, 30s
window, the same kind of measured recalibration as executor_oom's ramp redesign and
network_degradation's scrape_timeout fix earlier this session.

*Correction 2 (2026-07-13):* that fix only checked "does a real sample exist in this
window at all," not "does this window stay inside its own episode." [15s,30s]/30s needs
60s of lookback, and `modeling/window_horizon_sweep.py`'s 4-point sensitivity sweep
(bounded by real measured inter-episode gaps, the sweep this section already commits to
running) found the real tightest gap in EVERY class sits below that: broker_kill 47s,
executor_oom 32-33s, disk_pressure 49s, network_degradation 54s. Confirmed concretely,
not just in the abstract: executor_oom's ramptest4 (onset 17:17:38) had its widest
[15s,30s]/30s pre-failure window reach back to 17:16:38 — 28 seconds *before*
ramptest3's own OOM confirmation at 17:17:06. That window was drawing on the *previous
episode's own active fault*, not genuinely quiet pre-onset telemetry, for at least one
gap in every class. Recalibrated to **[10s, 15s] horizons, 15s window** (needs 30s of
lookback) — the largest grid point in the sweep that is contamination-safe for all 4
classes simultaneously, bounded by executor_oom's 32s tightest gap (the next grid point
up, 40s, is not safe for it). Every result in Section 6.5 was re-derived at this
corrected config before being reported as final — a gate-audit PASS computed partly
against a since-proven-unsafe configuration is not a PASS that should stand.

**Addendum — train/val/test methodology, decided before any window extraction, 2026-07-12.**

**1. Split unit is the fault episode, not the window.** Sliding-window extraction turns each
N=8-episode class into many more feature rows (multiple horizons × multiple normal-period
windows per episode). No window derived from a given rep's telemetry — pre-failure or
normal, at any horizon — may appear in more than one of train/val/test. Reason: windows
within a single episode are temporally autocorrelated and share episode-specific artifacts
(the same executor pod's exact JVM warm-up curve, the same specific noise draw in a given
netem session, the same broker restart timing) that a model could learn to recognize
instead of the general fault signature. Window-level random splitting would leak this
episode identity across splits and inflate apparent performance in a way that would not
generalize — standard practice (group-based/`GroupKFold`-style splitting), applied here
because the failure mode is concrete and checkable, not hypothetical.

**2. Leave-one-episode-out cross-validation, not a fixed ratio split — decided against a
fixed 5/1/2 split specifically because of what N=8 does to it, not by default convention.**
A fixed split with test=2 episodes can only produce recall/precision values in units of
50% (0/2, 1/2, 2/2) — too coarse to distinguish "the model generalizes" from "one held-out
episode happened to be easy or hard," and val=1 episode is not a meaningful basis for
hyperparameter selection (whatever hyperparameters happen to fit that one episode's
idiosyncrasies get picked, not necessarily the best general ones). Leave-one-episode-out CV
(8 folds/class: train on 7, test on 1, rotate) uses every episode as both training signal
and held-out evaluation across the full run, producing a much lower-variance aggregate
estimate than any single fixed split at this N, and is standard practice for exactly this
small-N regime (rare-event/medical ML literature, not invented for this project).
Hyperparameter tuning uses a nested inner loop (tune on the 7-episode training fold via
its own internal CV, never touching that fold's held-out episode) — outer-loop test
episodes stay genuinely unseen during tuning, not just during final fit.

**Deviation logged, 2026-07-12 (gate-auditor finding), closed same day.** the nested-
tuning design above was decided before window extraction started, but the Weeks 8-9
first pass initially did not implement it — every class was evaluated with one fixed,
untuned `RandomForestClassifier(n_estimators=200, max_depth=5, class_weight="balanced")`
configuration, no inner CV, no hyperparameter search. This was an unflagged gap between
documented methodology and actual implementation until gate-audit caught it.

**Implemented same day, `modeling/multi_model_nested_tuning.py`:** for broker_kill,
disk_pressure, and network_degradation (the 3 classes with 18 LOO groups — 8 real
episodes + 10 normal-reference windows), hyperparameters are now chosen via a genuine
nested CV — inner 5-fold `GroupKFold` on each outer fold's 17 training groups only
(full inner-LOO was tried first and costs ~2,750 fits/class just for RF; 5-fold cuts
that ~3.4x with no loss of principle), never touching that fold's held-out group.
executor_oom is still **not** nested-tuned. At the original N=8 collection (5 usable
episodes, 3 clean), only 4 (or 2, clean-3 subset) training groups remained per outer
fold — an inner search had no meaningful signal to select on. The 2026-07-13 top-up to
N=15 (11 usable episodes) raises the full-set count to 10 training groups per outer
fold, comparable to the other 3 classes' 17 — but the clean subset stayed at 2 episodes
(1 training group per fold) regardless of top-up, a structural pod-age ceiling, not a
sample-size one (Section 6.5's N=15 addendum). Nested tuning for the now-viable full
set was not requested or run in this pass; still an explicit exception, not a silently
carried-forward gap.

**3. Applied consistently across all 4 evaluated classes — with an explicit flag on
broker_kill, not glossed over.** broker_kill's own baseline evaluation
(`results/baseline-threshold-evidence/evaluation_output.json`) found real Prometheus
telemetry actually captured *inside* the fault window in only 1 of 8 episodes (tp=1, fn=7 —
the other 7 episodes' outages, 11-62s each, simply fell between scrapes at this pipeline's
60s interval; see `docs/baseline_thresholds.md` Section 1). This is a ground-truth-labeling
concern independent of split methodology, and LOO-CV does not fix it: in the 1 fold where
the single informative episode is held out as test, the training set has zero examples of
what a real captured outage looks like; in the other 7 folds, the training set has that one
informative episode but the test episode itself likely has no real captured signal to
detect. Either way, aggregate broker_kill ML metrics are very likely to come back near
chance/near the static baseline's own 12.5% regardless of model quality — not because ML is
inherently weak on this class, but because the underlying labeled data mostly lacks
observable signal at the current scrape resolution. **Recommendation: proceed with the same
LOO-CV methodology on broker_kill for consistency, but do not report its ML result as
directly comparable to the other 3 classes without this caveat attached.** If it comes back
uninformative as expected, the fix is the reactive top-up path already in Section 11's risk
register (collect more broker_kill reps, which — given the sparsity is a timing coin-flip,
not a fault-design flaw — improves the odds of capturing more informative episodes), not a
different split scheme. executor_oom (8/8 real fault-window signal, engineered ramp),
disk_pressure (8/8, deterministic single-sample-but-present-every-time), and
network_degradation (8/8 detection-level signal, though only 4/8 reach the separately-
defined severity threshold per `docs/baseline_thresholds.md` Section 5) do not share this
specific fragility — LOO-CV applies to them without the same caveat.

**4. Normalization/scaling is fit exclusively on each fold's training episodes, refit per
fold — never on the full N=8 before the LOO loop starts.** Fitting a scaler (mean/std, or
any other normalization statistic) on all 8 episodes' windows before splitting would leak
the held-out episode's own distribution into the statistics used to transform the training
data — a real, well-documented leakage bug (the model's inputs would already encode
information about the specific test episode it's about to be evaluated against), not a
theoretical one. Each of the 8 LOO folds fits its own scaler on that fold's 7 training
episodes' windows only, then applies the *same fitted* transform to the 1 held-out episode's
windows for evaluation. The held-out episode's data never contributes to the fit in any
fold. This applies to every fold independently — 8 separate scaler fits per class, not one
shared fit reused across folds.

### 6.4 Models and Baselines
- **ML models**: Random Forest, XGBoost/LightGBM on windowed statistical features (extends your existing Isolation Forest / DBSCAN anomaly-detection background into a supervised, lead-time-aware setting); optionally a lightweight temporal model (e.g., simple LSTM or GRU) as a stretch comparison if time allows.
- **Baseline (critical, non-negotiable)**: a static-threshold detector reproducing real alerting rules from current practice (consumer lag > X, under-replicated partitions > 0, executor memory > Y%) evaluated on the *same* fault-injection dataset. This is the baseline that makes the paper's claim testable — without it, "ML predicts failure" is an unfalsifiable claim.

**Multi-model comparison completed, 2026-07-12, re-run 2026-07-13 at the corrected
`[10s,15s]`/15s config (`modeling/multi_model_nested_tuning.py`,
`results/ml-first-pass/multi_model_nested_tuning.json`).** RF/XGBoost/LightGBM,
nested-tuned per the previous subsection. Headline finding, confirmed at both configs:
**model choice and hyperparameter tuning change no class's verdict.**

At the current `[10s,15s]`/15s config: disk_pressure is unambiguously high across all
three (RF/LightGBM F1=1.000, XGBoost F1=0.933); executor_oom's RF/LightGBM agree
exactly at 0.800/0.667 for the 5-episode/3-clean-episode subsets — **N=8 collection,
superseded 2026-07-13 by the N=15 top-up's single-RF-model re-run (F1=0.909/0.571,
Section 6.5's Final verdict table); the multi-model RF/XGBoost/LightGBM comparison
itself was not re-run at N=15, only the single fixed RF pipeline was, per the specific
scope of that follow-up** (XGBoost collapses to
0.000 on both — small-N model instability, not a new finding); broker_kill and
network_degradation are negative across every model with no feature-importance leak
flagged at this config (broker_kill de-confounded 0.762/p=0.26; network_degradation
RF/XGBoost/LightGBM all land in 0.370-0.429, p=0.87 for the best of them) — a *cleaner*
negative than the superseded config produced, not a weaker one.

**Historical note, superseded 2026-07-13 config (`[15s,30s]`/30s):** at that config, an
automatic feature-importance guard (n_samples importance > 0.5) built into the
evaluation script caught XGBoost's tuned config exploiting the n_samples grid-
misalignment leak *more aggressively* than RF/LightGBM's grids did on
network_degradation — F1=0.941 on the leaked feature set (confirmed genuinely
significant on that specific leaked configuration too: 0/100 shuffled≥real, p<0.01,
`network_degradation_xgboost_raw_significance.json`), collapsing to F1=0.759, p=0.06
once de-confounded. That near-miss does not appear at the corrected config — worth
keeping as a record that the guard worked, not as a live result to compare against.

No stretch temporal model (LSTM/GRU) attempted this pass — N per class (5-18 groups) is
too small to fit a temporal model meaningfully; not pursued, not silently dropped.

### 6.5 Metrics
Precision, Recall, F1, AUROC for the classification framing; **lead time** (median seconds between first true-positive alert and actual recorded failure onset) as the headline metric; false-positive rate under confirmed normal operation (this is what determines whether the approach is usable in practice — a model that predicts constantly is worthless even with perfect recall).

**Weeks 8-9 first-pass ML results, final four-class verdict (2026-07-12, fully
re-derived 2026-07-13 at the corrected config).** Window-level classification only
(lead time itself is Weeks 10-11's deliverable, not computed here). Fixed pipeline for
every class: `RandomForestClassifier(n_estimators=200, max_depth=5,
class_weight="balanced")`, features `[mean, std, min, max, last, n_samples]` per **15s
window at [10s, 15s] horizons** (corrected from the original [15s,30s]/30s per Section
6.3's second correction — the sweep below is what surfaced the need for it), LOO-CV per
Section 6.3's addendum. Every number below is traceable to a committed JSON in
`results/ml-first-pass/`, not asserted from prose.

*Three real bugs found and fixed while building this pass — a reviewer should be able to
follow this trail, not just trust the final table:*

1. **Metric-string escaping bug.** `CLASS_CONFIG`'s query templates in
   `modeling/extract_and_train.py` use `.format()`-escaped `{{...}}` braces, but only
   pod-scoped classes called `.format()` in the first draft — broker_kill's query was
   sent to Prometheus literally, returning HTTP 400. Fixed: always call
   `.format(pod=ep.get("target_pod", ""))` regardless of `pod_scoped`. Folded into
   commit `ae6b8e4` (never shipped as a broken intermediate state).
2. **n_samples window-size leak.** The first working extraction produced a suspicious
   uniform F1=1.000 across 3 of 4 classes. Inspecting raw features (not trusting a
   suspiciously clean result) found normal-reference windows were 300s wide (~21
   samples) against pre-failure's 30s (~3 samples) — `n_samples` alone perfectly
   separated the label, unrelated to genuine signal. Fixed: `NORMAL_REF_WINDOW_S =
   WINDOW_S` (30s, matched exactly). Commit `ae6b8e4`.
3. **Normal-reference stride / grid-misalignment bug.** Investigating disk_pressure's
   std-only false positives (below) found the normal-reference window loop still
   strode by `NORMAL_REF_WINDOW_S` (30s) after fix #2, packing all 10 "normal" windows
   into the *first 5 minutes* of the intended 67-minute quiet reference period — a
   slice that sits entirely inside executor_oom's own `ramptest3` fault window
   (injected 17:13:37, OOM 17:17:06). Every "normal" sample for broker_kill/
   disk_pressure/network_degradation (the three classes sharing this pooled-quiet-
   period code path) was drawn from inside a *different class's active fault*. Fixed
   with a separate `NORMAL_REF_STRIDE_S = 402` constant, spreading the 10 windows
   evenly across the full period. Commit `d75783f`. `executor_oom` draws normal
   context from each episode's own pre-injection period instead
   (`extract_executor_oom_normal_windows()`) and was never exposed to bug #3 (the
   stride bug) — confirmed empirically via `executor_oom_feature_importance_check.json`,
   whose `full_5_episode` block is bit-identical to the pre-existing snapshot committed
   at `63e2d8b`, which already postdates bug #2's fix; the stride fix (bug #3, `d75783f`)
   is the only one this specific empirical comparison isolates. The code-level argument
   (`extract_executor_oom_normal_windows()` never calls `extract_normal_reference_windows()`
   or references any `NORMAL_REF_*` constant) is what establishes immunity to bug #2 as
   well. Re-extracting broker_kill and network_degradation under the fix changed their
   numbers materially (`3423a6b`) — including a further, distinct false-positive result
   for broker_kill (below), caught the same way: by not trusting a suspiciously large
   F1 jump.
4. **Window/horizon reach-back contamination (2026-07-13, see Section 6.3's second
   correction for the full gap math).** Bug #3's stride fix addressed *normal*-window
   contamination; it never checked whether *pre-failure* windows reached back far
   enough to overlap the previous episode in sequence. `modeling/window_horizon_sweep.py`
   found the then-shipped `[15s,30s]`/30s config needed 60s of lookback, but the real
   tightest inter-episode gap in every class sits below that. All results in this
   section were re-derived at the corrected `[10s,15s]`/15s config before being
   reported as final — every number below is post-correction, not the numbers that
   originally passed gate-audit.

**Gate-auditor correction, 2026-07-12 (caught before push, not after):** the first draft
of this section called executor_oom "positive, with caveats" on F1 alone, without the
permutation/null test every other class received — the one class-level check this whole
pass consistently applied elsewhere and skipped here. gate-auditor flagged it as a FAIL:
executor_oom's F1 sits *below* a trivial always-predict-pre_failure baseline (0.842 vs a
computed 0.900 for the 5-episode set; 0.727 vs 0.833 for the 3-clean-episode set), and
the model misclassifies **both** normal windows in every configuration tried (0/2
specificity — it predicts pre_failure regardless of the actual window). Ran the same
rank-based shuffle test used for the other three classes
(`executor_oom_null_baseline_check.json`): 34/100 shuffled F1≥real (p=0.34, 5-episode),
54/100 (p=0.54, 3-clean) — both squarely in chance territory, the same range as
broker_kill (p=0.66) and network_degradation (p=0.56). The verdict below is corrected
accordingly, not softened with a caveat. (All numbers in this paragraph are from the
`[15s,30s]`/30s config in place on 2026-07-12; the *Final verdict* table below reflects
the 2026-07-13 re-derivation at the corrected `[10s,15s]`/15s config — the numbers moved,
the conclusion this paragraph describes catching did not.)

*Final verdict, at the corrected `[10s,15s]`/15s config:*

| Class | Verdict | Key numbers | Evidence file(s) |
|---|---|---|---|
| **executor_oom** | Negative, confirmed at N=15 — not a leak, not drift, not small-N power | LOO-CV F1=0.800 (5 episodes, precision=0.800/recall=0.800) / F1=0.667 (3 clean episodes, precision=recall=0.667) at the original N=8 collection — both *below* a trivial always-predict-pre_failure baseline (0.909 / 0.857 respectively). Permutation test: **48/100 and 56/100 shuffled F1≥real (p=0.48 / p=0.56, chance)**. Feature importance evenly spread on both subsets (top feature 0.19-0.21, no single-feature leak). Same null verdict as the superseded config (was F1=0.842/0.727, p=0.34/0.54) — the specific numbers moved with the window/horizon recalibration, the conclusion did not. **Topped up to N=15 real episodes, 2026-07-13** (`fault_injection/campaign.py --top-up-class executor_oom --top-up-target 15`, same gradual-ramp protocol, no config change): 11/15 episodes yield a usable window (4 lost to scrape-timing misses, same class of miss as N=8). Full 11-episode set: F1=0.909 vs. trivial-baseline F1=0.957 (still below); **50/100 shuffled F1≥real (p=0.500)**. Clean subset stayed at just 2 episodes (ramptest3, ramptest10 — the only two whose executor pod is ever old enough at onset for the normal-window definition to find a settled reading; every other episode's pod is too young, a structural constraint of the gradual-ramp fault design's tight inter-episode gap, not scrape-timing luck, and top-up added zero new clean episodes because of it): F1=0.571 vs. trivial 0.800, **52/100 shuffled≥real (p=0.520)**. Both squarely chance, consistent with N=8's p=0.48/0.56 — doubling real episode count did not move the p-value out of chance territory. **This resolves the small-N-power-problem question rather than leaving it open: the null was not an artifact of insufficient data, and further top-up under the same protocol would not fix the clean-subset ceiling either, since that ceiling is set by pod age at onset, not episode count.** | `loo_cv_results.json`, `executor_oom_clean_episode_check.json`, `executor_oom_feature_importance_check.json`, `executor_oom_null_baseline_check.json` (N=8, superseded), `executor_oom_n15_evaluation.json` (N=15, current) |
| **disk_pressure** | Positive, via delta features | Raw-feature LOO-CV is now **F1=1.000** (was 0.968 at the superseded config — the corrected, contamination-safe windows make the raw signal *cleaner*, not weaker). Std-only ablation dropped to F1=0.316, 68/100 shuffled≥real (p=0.68) — std alone is far less informative at 15s windows (too few real scrapes per window for variance to mean much; this is expected, not a regression, since the delta-feature result below never depended on std alone). Delta/relative-baseline features (unchanged construction — episode-own pre-injection baselines don't depend on window/horizon size): **F1=0.941, 0/100 shuffled≥real (p<0.01) — bit-identical to the superseded config's result.** Delta recall=1.0 guarantees every pre-failure window (all 8 real episodes, including the still-unexplained campaign2 outlier) is caught. | `magnitude_ablation_check.json`, `disk_pressure_delta_features.json` |
| **broker_kill** | Negative | Full-feature LOO-CV F1=0.667 (precision=0.818/recall=0.562) — feature importance shows `n_samples` still carries **100%** of importance (up from 96.9% at the superseded config, i.e. the leak is structurally present at any window size — the `up{}` metric is flat regardless of window, only the exact sample-count parity ratio changes). Excluding `n_samples`: **F1=0.762 (precision=0.615, recall=1.000), 25/100 shuffled≥real (p=0.25, chance)**. Still negative — the specific F1/p moved with the recalibration (was 0.222/p=0.66), the verdict did not. Ground truth itself mostly lacks observable signal at 60s scrape resolution for this class (Section 6.3 point 3); not an ML shortfall, not fixable by better features or a different window. | `loo_cv_results.json`, `broker_kill_no_nsamples_check.json` |
| **network_degradation** | Negative | Full-feature LOO-CV F1=0.414 (precision=0.462/recall=0.375) — *lower* than the superseded config's 0.759, consistent with that earlier number being partly a contamination artifact, not partly-real signal. Std-only ablation: real F1=0.118, **100/100 shuffled≥real (p=1.00)** — the real value is now at the very bottom of the shuffled distribution, the cleanest possible negative result in this whole pass. | `loo_cv_results.json`, `magnitude_ablation_check.json` |

**Window/horizon sensitivity sweep, 2026-07-12 (`modeling/window_horizon_sweep.py`,
`results/ml-first-pass/window_horizon_sweep.json`).** This sweep is what *caused* the
recalibration documented in Section 6.3, not just corroborating evidence collected
after the fact — worth being explicit about the sequence, since the numbers above are
the corrected numbers, and this sweep is why they needed correcting. 4-point grid from
20s to 60s of required lookback, bounded by real measured inter-episode gaps rather
than assumed ones: the then-shipped `[15s,30s]`/30s default needed 60s of lookback
while the tightest real gap in every class sits below that (broker_kill 47s,
executor_oom 32-33s×6, disk_pressure 49s, network_degradation 54s); each grid point's
per-class safety was computed and reported explicitly, not assumed. Fixed, untuned RF
throughout at each grid point, isolating window/horizon's effect from the
model-comparison question in Section 6.4.

The sweep's own G0-G3 sequence is itself evidence for the verdicts above, independent
of the full re-derivation:
- **disk_pressure** stayed robustly high across every window size and actually
  *peaked* (F1=1.000) at the safest, smallest windows, dipping only at the one risky
  point (F1=0.968) — a real signal doesn't need contamination to look strong.
- **broker_kill** climbed monotonically with window size across the full grid
  (0.222→0.667→0.889→0.941), rising well before its own contamination-risk flag turned
  on at the largest grid point — the trend starts, not just ends, with reach-back
  distance. **network_degradation** rose overall but non-monotonically
  (0.483→0.414→0.552→0.759), with its steepest jump landing exactly at the one grid
  point flagged AT RISK. Neither pattern reads as flat noise — the apparent "signal"
  for both classes tracked reach-back distance into the *previous* episode's own
  fault, not genuine precursor detection.
- **executor_oom** stayed flat (0.800-0.842) regardless of window size or
  contamination-risk status — consistent with the null-baseline finding that this
  class's problem is small N, not window/horizon choice; no grid point rescued it.

**This is not a "four classes, two positive two negative" result — it never was, and
the framing matters.** What was actually proven: **one class (disk_pressure) with
significance-tested positive predictive signal** (delta-baselined features, F1=0.941,
0/100 shuffled≥real, p<0.01, bit-identical across both the superseded and corrected
window/horizon configs — the single most stable result in this whole pass); **three
classes (broker_kill, network_degradation, executor_oom) with no signal surviving
rigorous testing, each for a distinct, documented reason** — broker_kill's apparent
signal is a resolved n_samples grid-misalignment leak (100% of feature importance at
the current config; de-confounded F1=0.762, p=0.25, chance), network_degradation shows
a confirmed absence of shape-independent signal (std-only F1=0.118, p=1.00 — the
cleanest negative in the pass), and executor_oom shows no demonstrated discrimination
above chance or a trivial constant classifier at this N (F1 below baseline in both
subsets, p=0.48/0.56). At the time this pass closed, this was flagged as a small-N
power problem left genuinely open (Section 11's risk entry: more episodes could still
change this) rather than a resolved negative. **Closed 2026-07-13:** topped up to N=15
real episodes and the full pipeline re-run — p stayed at chance (0.500/0.520 vs. the
original 0.48/0.56), so this is now a confirmed negative, not an open power question
(Section 6.5's N=15 addendum, Section 11's risk entry updated to reflect closure). All
four verdicts were reconfirmed, not just carried over, after a full re-derivation at the
corrected window/horizon config. Lead-time reconstruction (disk_pressure-only, Section
6.5's addendum below) and the executor_oom top-up (Section 11's risk entry, now closed
above) were the two follow-ups flagged as out of scope for this pass at the time it
closed — both have since been completed, on separate, later dates, not silently folded
into this pass's original numbers.
RF/XGBoost/LightGBM comparison, hyperparameter tuning, and a real window/horizon sweep
— the three gaps gate-audit originally flagged — are done, and the sweep's own finding
(the shipped config was itself unsafe) has
also been corrected and re-verified, not left as a caveat on top of stale numbers.

**Weeks 10-11 addendum — ML lead-time reconstruction methodology, defined before
computing anything, 2026-07-13.** Same checkpoint discipline as every phase before this
one (Section 6.3's split methodology was signed off before window extraction; here, the
lead-time methodology is defined and reported before it's run against real episodes).

**1. Scope: disk_pressure only.** The only class with significance-tested positive
signal (delta features, F1=0.941, 0/100 shuffled≥real, p<0.01 —
`disk_pressure_delta_features.json`). broker_kill, network_degradation, and
executor_oom get no lead-time computation under any framing — there is no validated
signal to reconstruct a detection timestamp from, and computing one anyway (e.g., "the
model would have fired at horizon X") would misrepresent a chance-level classifier as
having a meaningful lead time. Their comparison-table entries will read "ML lead time:
not computed — no signal survived significance testing," per instruction.

**2. Per-episode model: reuse the existing LOO-held-out fold, no retraining.**
disk_pressure's delta-feature LOO-CV (`disk_pressure_delta_features.py`) already trains
18 models, one per held-out group (8 real episodes + 10 synthetic `normal_reference`
windows). Lead-time reconstruction only concerns the 8 folds where a real episode
(campaign1/2/3/4/5/7/8, topup1) was the held-out group — each such fold's fitted
`RandomForestClassifier` and `StandardScaler` were trained with zero knowledge of that
episode's own data, matching the anti-leakage discipline already established (Section
6.3 addendum point 4). The classifier's feature vector never includes the horizon or a
timestamp — it only sees `[delta_mean, delta_min, delta_max, delta_last, std,
n_samples]` — so the exact same fitted model can be evaluated on windows at horizons it
was never trained on, without retraining or leakage. This is what makes reconstructing
a continuous detection curve from an 18-fold LOO setup valid at all.

**3. Reference "failure/severity event": the same severity-threshold crossing the
baseline detector is scored against, not injection onset.** `docs/baseline_thresholds.md`
Section 4 defines disk_pressure's severity-target as a drop of >2.90GB, and the
baseline's own reported lead time (0.0s, 8/8 reps) is measured from threshold-crossing
to that severity event — not from injection. Using the identical reference event
(`severity_utc`, `results/baseline-threshold-evidence/evaluation_output.json`'s
`severity_detail` array) is what makes the ML-vs-baseline comparison in item 4 apples-
to-apples; anchoring ML lead time to `injection_timestamp_utc` instead would silently
compare two different questions. **Real, measured onset→severity gaps per episode (not
a fixed offset — checked, not assumed):**

| episode | onset | severity_utc | onset→severity |
|---|---|---|---|
| campaign1 | 11:00:18 | 11:01:13 | 55s |
| campaign2 | 11:03:03 | 11:03:13 | 10s |
| campaign3 | 11:05:39 | 11:06:14 | 35s |
| campaign4 | 11:08:28 | 11:09:13 | 45s |
| campaign5 | 11:11:14 | 11:12:14 | 60s |
| campaign7 | 11:17:29 | 11:18:14 | 45s |
| campaign8 | 11:20:25 | 11:21:15 | 50s |
| topup1 | 14:33:37 | 14:33:52 | 15s |

This varies 10-60s per episode (fill duration + time-to-next-scrape, not a constant) —
already implies the model's existing [10s,15s]-horizon training windows (anchored to
onset) sit roughly 20-70s before the severity event depending on the episode, before
any backward scan even begins. Worth stating plainly now, not discovering as a surprise
once numbers come back.

**4. Continuous window scan: same feature construction, evaluated at a horizon
continuum instead of the training set's 2 fixed points.** For each held-out episode,
slide the identical `WINDOW_S=15s` window backward from onset in `STEP_S=15s`
increments (horizon = 15s, 30s, 45s, 60s, ... — matching this pipeline's real 15s
Prometheus scrape cadence, the same granularity every extraction in this project has
used; a finer step would not generally surface new real samples). At each horizon,
extract `[mean, std, min, max, last, n_samples]` from real Prometheus data exactly as
`window_features()` already does, compute the delta features using that episode's own
`baseline_avail_bytes` (real, ground-truth, already in `EPISODE_BASELINES` —
independent of horizon, so this doesn't change), scale with the held-out fold's already-
fitted `StandardScaler` (never refit), and get the held-out model's hard classification
(`predict()`, not a probability threshold — matching how every other result in this
pass was evaluated).

**5. Backward scan bound per episode: real measured gap to that episode's own previous
disk_pressure episode, capped at a practical ceiling.** Same anti-contamination
principle Section 6.3's second correction established for training windows — the scan
must not reach into the *previous* disk_pressure episode's own fault/recovery period.
Real gaps (`cleaned_up_utc` of episode *i*-1 to onset of episode *i*, already measured
this session): campaign1→2: 49s, campaign2→3: 84s, campaign3→4: 73s, campaign4→5: 59s,
campaign5→7: 254s, campaign7→8: 70s, campaign8→topup1: 11481s. Bound = `min(real gap,
300s)` — the 300s cap is deliberate, not arbitrary: it's >3x the longest lead time
observed anywhere in this project so far (executor_oom's baseline mean of 64.9s,
range 48-83s), so it can't truncate a real result, but it stops topup1's 11481s gap
(3+ hours) from turning into a scientifically meaningless "the model predicted
pre-failure 2 hours early" claim, and stops the scan itself from becoming an expensive,
pointless Prometheus query over a multi-hour range. **campaign1 has no preceding
disk_pressure episode in this dataset** (it's the first rep) — bounded instead at 73s,
the median of the other 7 episodes' own real gaps (a specific, computed, disclosed
judgment call, not a default silently reused).

**6. Earliest-positive selection rule.** Scanning outward from onset (horizon
increasing) toward the scan bound, the reported detection point is the **first
(chronologically earliest, i.e., largest-horizon) window at which the held-out model
predicts pre_failure** — matching Section 6.5's own stated headline-metric definition
("lead time... between *first* true-positive alert and actual recorded failure
onset"). If predictions flicker (a later, smaller-horizon window predicts normal again
after an earlier positive), the first positive is still what's reported — an
operational alert fires once and doesn't wait for a stable run of positives before
being trusted. This is stated now so the rule isn't picked reactively after seeing
which choice produces a nicer number.

**7. No-detection handling.** If a held-out model never predicts pre_failure anywhere
in its episode's scanned range, that episode reports "no lead time recoverable" —
excluded from the mean/range calculation but explicitly counted (e.g., "lead time
computed for 6 of 8 episodes"), the same disclosure standard `baseline_thresholds.md`
already uses for its own "gaps checked: 4 of 6" reporting. Not silently dropped, not
treated as a lead time of 0.

**8. Reported outputs, once run:** per-episode lead time (up to 8 values, unrecoverable
ones flagged not averaged in), mean, range, and the count of episodes with a
recoverable detection — same rigor as the baseline detector's own lead-time reporting
in `docs/baseline_thresholds.md`. Lead time for a given episode = `severity_utc` minus
that episode's earliest-positive window's end timestamp.

**Correction found before the first real run, 2026-07-13 (user review, not
self-caught):** point 5's backward-scan bound originally checked only disk_pressure's
own previous episode. That misses episodes of *other* classes running in the same
tight testing window — exactly the class of bug already found once this session
(executor_oom's ramptest3 overlapping the old normal-reference windows). Recomputed
using every episode's real timestamp across all 5 classes: disk_pressure's campaign1 is
preceded by **backpressure_cascade's campaign8, ending only 46s before campaign1's own
onset** (not the 73s same-class-median placeholder originally used), and — more
consequentially — **campaign7's true nearest neighbor turned out to be a *discarded*
disk_pressure repetition (`campaign6`, excluded from the active N=8 for an unconfirmed
`drop_confirmed_utc` timestamp, not because nothing happened), ending 73s before
campaign7's onset, not the 254s gap to the previous *active* disk_pressure episode.**
Discarded reps still had a real fault physically injected — their telemetry is real and
present in Prometheus regardless of dataset-inclusion status. `modeling/
disk_pressure_lead_time.py`'s neighbor search now includes `results/campaign-n8/
_discarded/` for exactly this reason.

**Results, `modeling/disk_pressure_lead_time.py` → `results/ml-first-pass/
disk_pressure_lead_time.json`:**

*Flagged-episode trace (item 4 of the review), both resolved before any number was
trusted:*
- **campaign7 — confirmed contamination, corrected, not just excluded.** Under the
  original (same-class-only) 254s bound, the earliest positive landed at horizon=240s
  (window_end 11:13:29). Traced: that window's raw mean (995,148,230,656.0) is
  byte-for-byte identical to discarded campaign6's own `baseline_avail_bytes`
  (995,148,230,656.0); the next cluster back (h=75-120s window) matches campaign6's
  `post_fill_avail_bytes` (991,926,218,752.0) exactly. The model wasn't detecting a
  campaign7 precursor at all — it was reading campaign6's own real fill-and-cleanup
  telemetry. With the corrected 73s bound (bounded by campaign6, found via the fix
  above), the scan never reaches that contaminated region: campaign7 now resolves
  cleanly to horizon=60s, lead_time=105s — landing inside the same 55-110s range as
  every other clean episode, which is itself corroborating evidence the correction is
  right, not just defensible in isolation.
- **topup1 — flagged, traced, NOT contamination, but still excluded as unresolved.**
  No episode of any class (active or discarded) falls within topup1's true 630s gap to
  the previous episode (network_degradation's campaign8) — the practical 300s cap
  applies, and the model predicts pre_failure at every single scan point out to that
  cap, hitting the flag threshold by saturating it rather than by finding a genuine
  detection point. Traced the real underlying Prometheus series (not just the
  window-aggregated mean): a real, small, monotonic decline from 995,052,072,960 to
  995,050,299,392 bytes (≈1.77MB) over the full 5 minutes before onset — not noise, not
  a different episode's fault, just an unusually stable pre-injection period for this
  specific rep. Excluded from the headline numbers: not confirmed genuine (no negative
  region was ever found to bound it), not contamination either — a third, honestly
  labeled category, not forced into one of the other two to make the count come out
  even.

**A more consequential finding surfaced while resolving the above, worth stating
plainly rather than only in a footnote:** every one of the 7 clean episodes' earliest
positive prediction lands at *exactly* the last scannable point before that episode's
own contamination-safety bound (campaign1: 45s of 46s available; campaign2: 45s of 49s;
campaign3: 75s of 84s; campaign4: 60s of 73s; campaign5: 45s of 59s; campaign7: 60s of
73s; campaign8: 60s of 70s) — **zero episodes show a negative (correctly-normal)
prediction anywhere within their safe scan range.** That is a systematic pattern, not
scattered noise. It means the reported lead times are a *lower bound* set by how far it
was safe to look, not a *ceiling* reflecting the model's true detection horizon — the
model may be able to detect much further back, or (the less flattering but equally
plausible read) it may simply be classifying "resembles this episode's own baseline
range" rather than genuinely escalating precursor signal, and would keep predicting
positive indefinitely into any sufficiently quiet pre-injection stretch, exactly as
topup1's 300s-saturated, never-negative scan suggests. This is disclosed as an explicit
caveat on every number below, not resolved by scanning further this pass (each larger
bound needs the same discarded-episode-aware contamination check redone, real
additional work, not assumed safe by extrapolation).

**Final disk_pressure ML lead-time distribution (7 of 8 episodes; topup1 excluded,
above):**

| episode | earliest horizon before onset | lead time (vs. severity_utc) |
|---|---|---|
| campaign1 | 45s | 100s |
| campaign2 | 45s | 55s |
| campaign3 | 75s | 110s |
| campaign4 | 60s | 105s |
| campaign5 | 45s | 105s |
| campaign7 | 60s (corrected) | 105s |
| campaign8 | 60s | 110s |

**Mean: 98.6s. Range: 55-110s. n=7/8 (topup1 excluded as unresolved, not averaged in
and not scored as 0).**

**Mechanism trace, 2026-07-13 (user review, before this number could be trusted or
described in the write-up) — this reframes the claim, it does not just add a caveat.**
For all 7 episodes, pulled the earliest-positive window's raw `avail_bytes` (not just
the classifier's binary output) and compared to that episode's own settled baseline:

| episode | raw avail_bytes at detection | baseline_avail_bytes | delta (raw − baseline) |
|---|---|---|---|
| campaign1 | 995,153,293,312 | 995,152,609,280 | **+684,032** |
| campaign2 | 995,152,027,648 | 995,152,027,648 | 0 |
| campaign3 | 995,151,241,216 | 995,150,802,944 | **+438,272** |
| campaign4 | 995,150,311,424 | 995,149,934,592 | **+376,832** |
| campaign5 | 995,149,176,832 | 995,148,972,032 | **+204,800** |
| campaign7 | 995,147,304,960 | 995,146,944,512 | **+360,448** |
| campaign8 | 995,146,117,120 | 995,145,748,480 | **+368,640** |

**avail_bytes is not declining toward the fault in any of the 7 windows — every delta
is zero or positive** (more free space than the eventual baseline, the opposite of
what a physical precursor to a disk-*filling* fault would show), and every magnitude
(0-684KB) is 0.00002-0.00007% of the disk — three to four orders of magnitude below
the ~3.22GB real fault drop (`docs/baseline_thresholds.md` Section 4). Each window's
`std=0.0, n_samples=1` — a single real Prometheus scrape, no dispersion — so
`delta_mean=delta_min=delta_max=delta_last` at every one of these instances by
construction; there is no rate-of-change or slope feature in this feature set to
detect a trend even if one existed.

**Per-instance ablation, not aggregate training importance, to find what actually
drove each decision (`modeling/disk_pressure_lead_time_ablation.py` →
`results/ml-first-pass/disk_pressure_lead_time_ablation.json`, committed after
gate-audit flagged this was otherwise asserted from prose):** swept `delta_mean` (with
its three collinear siblings) through a fine 5KB-resolution grid for every one of the 7
held-out fold models, holding `std=0, n_samples=1` fixed at their observed values.
**Every fold's decision boundary lands at exactly -350,000 bytes, with zero variation
across folds (std=0 across the 7 thresholds)** — the classifier predicts pre_failure
whenever raw `avail_bytes` is at or within 350KB below *some* baseline-like reference
value, and predicts normal otherwise. This is a static absolute-value-proximity
threshold, not a trend detector — `std`/`n_samples` were confirmed not to matter by
direct swap (both classes commonly show `std=0, n_samples=1` at single-sample windows,
so they carry no discriminating information here). All 7 real deltas (0 to +684,032)
sit comfortably on
the positive side of every fold's -350,000 threshold — none is a close call.

**Item 3 (harness-setup timing) checked and ruled out, not assumed:** read
`fault_injection/disk_pressure.py` directly — `baseline_avail = avail_bytes(port)` is
queried immediately before the `dd` injection command with no intervening sleep or
extended setup phase (the only prior step is starting a port-forward, ordinary tooling
overhead, not something that leaves a multi-minute disk-usage signature). No
script-driven procedural mechanism explains the 45-84s timing. The actual explanation
is the real Prometheus scrape cadence for this metric (distinct real samples land
roughly 45-90s apart, confirmed by de-duplicating raw range-query output for topup1
earlier this pass) combined with where the ≈350KB threshold happens to sit relative to
ordinary background disk-usage drift (a few hundred bytes/second, unrelated to the
fault) — not a harness artifact, but not a physical fault signature either.

**Verdict: unresolved artifact, not genuine physical precursor signal, and not
harness-setup-timing correlated.** None of the three categories fits cleanly except by
elimination — it is closest to "unresolved artifact," except the mechanism itself is
now well understood, not mysterious: the classifier has learned a coarse "does this
absolute value resemble *some* disk_pressure-campaign-era baseline" discriminator
(baselines cluster tightly across all 8 episodes, 995.05-995.2 billion bytes, distinctly
above the quiet-reference period's 994.9 billion) rather than anything resembling
escalating fault detection. This is very likely the same underlying mechanism behind
the original Weeks 8-9 significance test at the trained 10-15s horizons too, not a
separate phenomenon specific to the extended lead-time scan — that test's windows are,
by the same construction, also just "close to this episode's own baseline" vs. "close
to the quiet-period's different baseline." **"Early warning of physical disk pressure"
and "reliably distinguishes disk_pressure-campaign-era telemetry from quiet-reference-
period telemetry" are different claims, and the evidence in this section supports only
the second one.** The 98.6s figure describes how far back that era-distinction remains
detectable within a contamination-safe window, not a physical detection horizon — RO3
asked whether ML can predict failure onset with a meaningfully positive lead time, and
this mechanism trace does not support answering yes on the strength of this number.

**ML-vs-baseline comparison table, all 5 fault classes:**

| class | baseline lead time | ML lead time | verdict |
|---|---|---|---|
| disk_pressure | 0.0s (8/8 reps — crossing coincides with severity on the identical real sample every time; no intermediate real data point exists for a raw threshold to fire earlier) | 98.6s mean, range 55-110s (n=7/8) — **not comparable as a detection-lead-time claim, see mechanism trace above** | **No meaningful ML-vs-baseline comparison can be drawn from this number.** The classification signal is real and significance-tested (p<0.01), but per-instance ablation shows it is a static, era-level absolute-value discriminator, not an escalating physical precursor to the fault — none of the 7 "detections" show avail_bytes declining, and the ≈350KB decision margin is three to four orders of magnitude below the fault's real ~3.22GB magnitude. The honest finding is that this pipeline's telemetry lets a classifier tell "disk_pressure recording session" apart from "quiet reference period," not that it gives 98.6s of physical early warning — reporting the latter as an ML-vs-baseline win would overclaim what Sections 6.4-6.5 and this trace actually established. |
| broker_kill | 57.0s (n=1 — only 1 of 8 reps ever captured a real in-fault sample at this pipeline's 60s scrape interval) | not computed — no signal survived significance testing | **No comparison possible.** ML has no validated signal to compare (Section 6.5, F1=0.762 de-confounded, p=0.25, chance). The baseline's own 57s figure is a single data point from 12.5% recall, not a reliable number to beat or lose to either. |
| network_degradation | 0s in 3 of 4 reps that reach severity, 65s in the 4th; only 4 of 8 reps reach the severity bar at all | not computed — no signal survived significance testing | **No comparison possible.** ML has no validated signal (F1=0.118 std-only, p=1.00 — the cleanest negative in the whole Weeks 8-9 pass). |
| executor_oom | 64.9s mean, range 48-83s (n=8/8 — the strongest, most consistent baseline lead time of any class) | not computed — no signal survived significance testing | **No comparison possible.** ML has no validated signal despite this class having the best baseline; the static-threshold detector remains the only validated detector here. Originally attributed to a small-N power problem — **closed 2026-07-13**: topped up to N=15 real episodes, full pipeline re-run, p stayed at chance (0.500/0.520 vs. the original 0.48/0.56). Confirmed negative, not a power question — more episodes would not be expected to change this verdict (Section 6.5's N=15 addendum). |
| backpressure_cascade | excluded — no independently-scraped Prometheus metric exists for Spark processing lag on this deployment; the only lag field is the driver-log-parsed one this class's own ground truth already uses, so any "detector" built on it would be circular (`baseline_thresholds.md` Section 3) | not computed — same reason, carried forward | **No comparison possible, same root cause at both stages.** This class was never modeled in Weeks 8-9 either, for the identical reason: no independently-scraped telemetry exists to build features from in the first place, not a modeling failure. |

**Reading this honestly:** zero of the 5 classes support an "ML gives meaningfully
positive lead time" claim once disk_pressure's own mechanism is traced through, not
one win and three non-comparisons. broker_kill, network_degradation, and executor_oom
have no validated classification signal at all (Weeks 8-9), so no lead time was ever
computable for them — honest non-comparisons, not losses. disk_pressure *does* have a
validated, significance-tested classification signal (p<0.01, reproducible across two
window/horizon configs, corroborated by the sensitivity sweep) — but this pass's
mechanism trace shows that signal is a static absolute-value discriminator between two
recording sessions, not an escalating physical precursor, so the 98.6s figure it would
otherwise produce cannot honestly be reported as a lead-time win either. Weeks 10-11's
actual finding is negative across the board on RO3's specific question ("can ML predict
failure onset with a meaningfully positive lead time") — a real, defensible, and
disclosed result in its own right, not a null turn that needs padding.

### 6.6 Explainability
SHAP or permutation feature importance per fault class, to show *which* signals drive early warning for *which* failure type — this is the actionable-insight narrative for the write-up and for real operator adoption.

**Weeks 12 result, 2026-07-13 — disk_pressure only.** SHAP is only meaningful applied to
a model with a validated classification signal to explain; of the 5 fault classes, only
disk_pressure clears that bar (F1=0.941, 0/100 shuffled≥real, p<0.01 — Section 6.5).
broker_kill, network_degradation, executor_oom, and backpressure_cascade have no
significance-tested signal at any N reached so far, so there is no fitted decision
boundary worth explaining for them, and none was computed — same discipline as Weeks
10-11's lead-time scope rule (Section 6.5's addendum, point 1).

`modeling/disk_pressure_shap.py` runs `shap.TreeExplainer` (exact for tree ensembles,
no sampling approximation) against the identical 8 real-episode LOO folds from
`disk_pressure_delta_features.py` — same `X`/`y`/`groups`, same
`RandomForestClassifier(n_estimators=200, max_depth=5, random_state=42,
class_weight="balanced")`, same per-fold `StandardScaler` — explaining the actual
reported F1=0.941 model, not a stand-in trained separately for this check. Explained all
16 true-positive `pre_failure` windows (recall=1.0, matches Section 6.5's delta-feature
result).

**Attribution result:** magnitude features (`delta_mean`/`delta_min`/`delta_max`/
`delta_last`) carry **97.4%** of mean |SHAP| combined (delta_mean alone 27.2%, followed
by delta_min 25.1%, delta_last 23.0%, delta_max 22.1%); `std` and `n_samples` carry
0.7% and 1.8% respectively — negligible, and consistent with Weeks 8-9's separate
n_samples-leak check staying clean at the delta-feature level too. A pre-registered gate
(magnitude share ≥80%, no single non-magnitude feature ≥15%) was checked in code before
any figure was produced — passed at 97.4%/1.8%, not a borderline call requiring
judgment.

**This independently corroborates the Weeks 10-11 mechanism trace, via a different
method on different (real classification, not synthetic-sweep) data.** The ablation
(`disk_pressure_lead_time_ablation.py`) found the lead-time-reconstruction model's
decision boundary sits at a static ≈-350,000-byte threshold with zero cross-fold
variance. SHAP here shows the *same* qualitative mechanism from the classification
side: two further observations beyond the headline attribution percentages make this
more than a restated conclusion —

1. **The raw delta values at the actual trained horizons (10s/15s pre-onset) are all
   ≥0** (range 0 to +684,032 bytes across the 16 true-positive windows) — avail_bytes
   has not started declining at the windows the classifier is actually evaluated on.
   There is no downward trend present in this data for any feature to detect, let alone
   a rate-of-change one (`DELTA_FEATURE_COLS` has no slope/derivative term — confirmed
   by inspection, not assumed).
2. **SHAP attribution vectors are near-identical across episodes with very different
   raw delta values** — e.g. campaign1 (delta_mean=+684,032B) and campaign2
   (delta_mean=+0B) produce almost the same per-feature SHAP vector at both horizons.
   Checked directly, not assumed a coincidence: the two folds' fitted scalers
   (`StandardScaler.mean_`) and tree thresholds are genuinely different (verified by
   direct comparison), but every real observed delta value sits so far above the
   ablation's ≈-350,000-byte boundary that all 16 instances land on the identical side
   of every split that matters, producing the same leaf path regardless of exactly how
   far above threshold each one sits. This is the fingerprint of a coarse, single-
   threshold step function, not a classifier sensitive to graded magnitude — the same
   qualitative shape as the ablation's zero-cross-fold-variance finding, arrived at from
   the model's actual behavior on real data rather than a synthetic parameter sweep.

**Verdict: no discrepancy found.** SHAP attributes the classification decision to
static magnitude/proximity features (97.4% combined share) with no trend-like or
unexpected non-magnitude attribution, matching what the mechanism trace already
concluded — the F1=0.941 result is a real, significance-tested "which recording era"
discriminator, not evidence of an escalating physical precursor signal, and this
finding does not change the Section 6.5 comparison table's verdict for disk_pressure.
Evidence: `modeling/disk_pressure_shap.py`,
`results/ml-first-pass/disk_pressure_shap.json` (full per-instance SHAP breakdown, gate
check, sign check), `results/ml-first-pass/disk_pressure_shap_summary.png` (mean-
attribution bar chart + per-instance beeswarm, the Section 12 candidate figure).

## 7. Novelty and Expected Contribution

1. First reproducible fault-injection benchmark specifically targeting Kafka + Spark Structured Streaming pipeline failure prediction, as distinct from data-stream anomaly detection and from Spark job runtime/performance prediction (both separately, differently studied).
2. Lead-time-based evaluation methodology — not just point-in-time classification accuracy — directly answering the practical "does this actually help operators" question that a pure-accuracy paper cannot.
3. Head-to-head comparison against static-threshold alerting, i.e., against what the field actually does today, grounding the contribution in demonstrated practical value rather than an isolated metric improvement.
4. A fully open, reproducible benchmark (infra-as-code + fault-injection scripts + labeled dataset) — the benchmark itself is a citable artifact independent of the specific model results, mirroring how SCG/SupplyGraph's value lay partly in being public and reproducible.

## 8. Work Plan (14–16 weeks — full cost, infra built from zero)

| Weeks | Phase | Deliverable |
|---|---|---|
| 0–1 | **Infra build (new — not previously scoped)** | Kind cluster up; Kafka broker running; Spark Structured Streaming job consuming from it; Prometheus scraping Kafka JMX + Spark metrics + node exporter. Gate: a single pilot fault (broker kill) shows a real signal change in Grafana/Prometheus — verified visually, not just "command ran." |

**Phase 0 execution status (as of 2026-07-08, updated live — not retroactively):**
- DONE, verified: Kind cluster, single-broker KRaft Kafka (Strimzi Kafka Operator, `infra/kafka/`), Prometheus scraping the broker's JMX exporter via pod-based service discovery (`infra/prometheus/`). Bitnami's Kafka image is confirmed dead (Broadcom paywall, 0 public tags) — pivoted to Strimzi per the pivot rule, same session.
- DONE, verified: pilot fault injection (broker pod delete) with a real, independently-corroborated signal change captured in Prometheus and Kubernetes events — see `results/phase0-pilot-fault/`. This satisfies the gate condition's literal text ("pilot fault shows a real signal change, verified visually") for the Kafka+Prometheus scope built so far.
- DONE, verified: Spark Structured Streaming job (`infra/spark/`) consuming real messages from the Kafka topic via a continuous producer (`infra/kafka/producer-loadgen.yaml`). Evidence persisted at `results/phase0-spark-metrics/driver_log_excerpt.txt` (real per-window event counts, batches 114-116 of 116+ observed). Deployed via `spark-submit --deploy-mode cluster` directly against the K8s API (not the Spark Operator -- unnecessary extra surface for a single always-on job at this stage), using a pod template to work around a real Spark 4.1.2 limitation (`spark.kubernetes.driver.volumes.configMap.*` conf keys don't exist -- only hostPath/emptyDir/nfs/persistentVolumeClaim are supported that way; ConfigMap mounts require a pod template, confirmed against the official docs, not assumed). Both required ConfigMaps are committed as declarative manifests (`infra/spark/configmap-script.yaml`, `infra/spark/configmap-driver-pod-template.yaml`), content-verified identical to what's live in the cluster -- the Spark layer is reproducible from `kubectl apply -f` in the order documented in `infra/spark/submit-pod.yaml`, not from undocumented imperative commands.
- DONE, verified: Prometheus scraping Spark's built-in `spark.ui.prometheus.enabled` servlet (`/metrics/executors/prometheus/` on the driver, pod-discovery scrape job same pattern as Kafka). Evidence persisted at `results/phase0-spark-metrics/metrics_executor_query.json` (88 series total across 47 distinct metric names, of which 41 are `metrics_executor_*`; the rest are `spark_info`, `up`, and `scrape_*` meta-series) and `results/phase0-spark-metrics/targets_health.json` (target health=up for both `kafka-broker-jmx` and `spark-driver`).
- DONE, verified: node exporter enabled (`prometheus-node-exporter.enabled=true`). Evidence persisted at `results/phase0-node-exporter/node_memory_available_query.json` (real non-zero `node_memory_MemAvailable_bytes`) and `results/phase0-node-exporter/node_exporter_all_metrics_sample.json` (1259 series, 313 distinct metric names).
- Full Phase 0 deliverable list for this row is now complete. Known operational note (not a gate blocker): the pilot Structured Streaming job's 5s micro-batch trigger interval is undersized for this Kind node's resources -- observed batch durations of 16-27s, fluctuating rather than monotonically climbing (no OOM, no restarts across 20+ minutes). Worth widening the trigger interval before the real fault-injection campaign; not addressed now since it doesn't block this checkpoint.
- Mid-build incident: the Kind cluster was unexpectedly destroyed mid-session (Docker/WSL2 restart wiped the container; a sibling, unrelated Kind cluster on the same machine survived because it had a restart policy applied and ours didn't). No data was lost -- everything needed was already captured as code/evidence in git -- but the entire stack (Kind, Strimzi, Kafka, Prometheus, producer) had to be rebuilt from the committed IaC mid-session. This is itself a small piece of evidence for why infra-as-code + committed evidence (not just a running cluster) is the right discipline here.
- Gate-audit correction, 2026-07-11: Criterion 5 (maxNumFailures fix) was recorded as PASS on 2026-07-11 (~15:16 UTC) based on config-presence verification only (reading the loaded spark.properties file). This was a false PASS -- the config key was invalid (spark.kubernetes.executor.maxNumFailures does not exist; correct key is spark.executor.maxNumFailures) and Spark silently ignored it. Caught 2026-07-11 (~16:34 UTC) when a 4th executor kill crossed the old default-3 boundary during unrelated ramp-calibration work. Lesson generalized: config-presence checks are insufficient for silently-ignored-flag risks; behavioral confirmation is required for any config claiming to change runtime failure-tolerance behavior. Remediated the same day: corrected key set in `infra/spark/submit-pod.yaml` (commit `3731a61`, 16:47:40 UTC), re-verified against a live driver's loaded properties file (`results/phase1-gate-evidence/live_maxnumfailures_check_CORRECTED.txt`), and behaviorally confirmed by 8 consecutive executor kills with 0 driver crashes across the redesigned executor_oom campaign (`ramptest3`-`ramptest10`) -- config-presence plus behavioral confirmation, not presence alone.
- Provenance correction, 2026-07-11: because the maxNumFailures bug had driver-lifetime scope (not local to executor_oom), the other 4 fault classes' original collection manifests were independently cross-checked for executor/driver pod-loss failures rather than trusting "none reported." Found one: `backpressure_cascade` rep1 (original campaign run) failed with "no running driver pod found" at 10:49:32 UTC, immediately after the executor_oom class had already burned through 5 real executor kills against the true (silently-defaulted) maxNumFailures ceiling of 3 -- a driver exhausting its failure budget and going down right as the next class started is the coherent explanation, though not provable after the fact since no driver log was being captured for that window yet. Its topup1 replacement (15:00:20 UTC) was itself collected between the wrong-key "fix" (14:53:09 UTC) and the real fix (16:47:40 UTC), so it was also contaminated. Replaced with a fresh rep (`refresh1`, 18:15:49 UTC, well after the real fix); old rep discarded to `results/campaign-n8/_discarded/backpressure_cascade-unconfirmed-config/README.md` on provenance grounds, not because the data point itself was bad. The other 3 classes (broker_kill, disk_pressure, network_degradation) showed no such failures in their original collection windows. Explicitly re-checked `disk_pressure`'s topup1 (14:33:37 UTC) and `network_degradation`'s full 8-rep re-run (14:03:20-14:23:07 UTC) against the *actual* risk window -- not just between the wrong-key commit (`3c627a0`, 14:53:09 UTC) and the real fix (`3731a61`, 16:47:40 UTC), since before `3c627a0` there was no override at all and the true default-3 ceiling was silently active the whole time, same functional risk. Both reps' timestamps fall before `3c627a0` entirely, inside that wider risk window. Both classes are structurally immune regardless: neither `disk_pressure.py` nor `network_degradation.py` ever queries a Spark driver/executor pod (both operate purely on the Kafka broker pod and Prometheus/node-exporter metrics -- confirmed by grep, zero references to `spark`/`driver_pod`/`find_driver`/`find_executor` in either script), and their run records (original and topup/rerun alike) show zero errors of any kind. Clean, not by luck of timing but by the fault mechanism never touching the thing that broke.
| 2–3 | Saturation re-check + fault taxonomy lock + injection tooling | Confirmed gap still open; fault-injection scripts working end-to-end for at least one fault class |
| 4–5 | Full fault-injection campaign | Labeled dataset across all fault classes, N=8/class (revised down from initial N=15-20 target — justified by per-class CV analysis, see commit history; reactive top-up only if a specific class shows instability during modeling, not a blanket rescale), ground-truth timestamps recorded |
| 6–7 | Baseline implementation | Static-threshold detector reproducing real alerting rules, evaluated on the dataset |
| 8–9 | ML models | RF / XGBoost / LightGBM trained, tuned, window/horizon sensitivity swept |

**Weeks 8-9 accepted limitation, logged 2026-07-12 (gate-auditor finding, not
self-reported before audit) — closed same day.** The first pass initially delivered
against this row only partially: Random Forest trained and evaluated via LOO-CV across
all 4 viable classes with a full verification trail (Section 6.5), but no
XGBoost/LightGBM, no hyperparameter tuning, and no systematic window/horizon
sensitivity sweep.

**All three gaps closed same day, same rigor as the rest of this pass:**
(a) XGBoost/LightGBM trained and nested-tuned alongside Random Forest
(`modeling/multi_model_nested_tuning.py`, Section 6.4) — model choice changes no
class's verdict, and the multi-model run's own automatic leak guard caught XGBoost
exploiting the same n_samples artifact more aggressively than RF/LightGBM's grids did
on network_degradation, correcting it before it could be reported as a new finding;
(b) nested hyperparameter tuning implemented via inner 5-fold GroupKFold for the 3
classes with enough LOO groups to support it (Section 6.3's 2026-07-12 deviation note,
now marked closed) — executor_oom is a permanent, principled exception at this N, not
a remaining gap; (c) a real 4-point window/horizon sweep, bounded by measured
inter-episode gaps (`modeling/window_horizon_sweep.py`, Section 6.5) — which itself
surfaced a new, disclosed finding: the shipped `[15s,30s]`/30s default is not
contamination-safe for any class at its tightest real gap, and the sweep shows the
negative classes' apparent signal tracking that contamination risk directly.

**Follow-up, 2026-07-13:** the sweep's own finding was then acted on, not left as a
disclosed caveat sitting on top of a known-unsafe default. Recalibrated to the sweep's
own `[10s,15s]`/15s grid point (Section 6.3's second correction) and re-derived every
number in Sections 6.4 and 6.5 at the corrected config before calling this row done —
a gate PASS computed partly against a since-proven-unsafe configuration would not have
been a PASS that should stand. Weeks 8-9 is now fully complete against this row's
original criteria, verified at a configuration confirmed safe for every class, not
carried forward and not left resting on stale numbers.
| 10–11 | Lead-time evaluation | Full metric suite computed per fault class; ML vs. baseline comparison table |
| 12 | Explainability | SHAP analysis per fault class |

**Weeks 12 complete, 2026-07-13.** SHAP analysis run against disk_pressure's
delta-feature LOO-CV models (Section 6.6) — the only class with a validated
classification signal to explain, per the same scope discipline used for Weeks 10-11's
lead-time reconstruction. Result: SHAP attribution independently corroborates the
Weeks 10-11 mechanism trace (magnitude features 97.4% combined share, no trend-like or
unexpected non-magnitude attribution) rather than contradicting it — the pre-registered
gate for producing a paper figure passed, and the figure
(`results/ml-first-pass/disk_pressure_shap_summary.png`) is the Section 12 deliverable.
No SHAP was computed for broker_kill, network_degradation, executor_oom, or
backpressure_cascade — none has a significance-tested signal at any N reached so far,
so "SHAP analysis per fault class" is satisfied as "per fault class with something
valid to explain," not silently narrowed without comment.
| 13–16 | Writing | Full draft, review/audit pass |

## 9. Target Venues

IEEE Access, MDPI Electronics or Applied Sciences (rolling submission, consistent with your existing KSPFail venue targeting), or an AIOps/SRE-adjacent workshop track at a systems or big-data conference. No hard external deadline identified — treat as rolling, consistent with prior KSPFail scoping.

## 10. Anticipated Reviewer Concerns and Mitigation

| Concern | Mitigation already built into design |
|---|---|
| "Isn't this just anomaly detection with extra steps?" | Explicit lead-time framing plus fault-injection ground truth plus head-to-head threshold comparison — none of which a generic anomaly-detection paper reports |
| "Single small Kind cluster — does this generalize to production-scale Kafka/Spark?" | Framed explicitly as a methodology/benchmark contribution, same defense pattern as SCG-based work being a single-company case study; future work explicitly invites replication at production scale |
| "Why not just tune the alerting thresholds better instead of using ML?" | This is literally the comparison the paper runs — the lead-time delta between tuned thresholds and ML is the reported result, not an assumption |
| "Synthetic fault injection — do injected faults resemble real production incidents?" | Fault taxonomy drawn directly from documented real-world Kafka/Spark operational failure modes (vendor docs, SRE literature), not arbitrary; documented as a limitation, not hidden |
| "Small sample per fault class — statistical power?" | Report confidence intervals / repeated-run variance explicitly rather than single-run point estimates; treat as a documented limitation with a stated minimum N |

## 11. Risk Register (project-execution risks, not paper-review risks)

- **Fault-injection tooling friction** (Chaos Mesh setup complexity on Kind) — RESOLVED 2026-07-08: adopted manual `kubectl` / `tc netem` scripts as the locked choice from the start (see Section 6.2), not a reactive fallback. The Phase 0 pilot already proved this approach end-to-end with strong evidence quality, so there was no reason to pay Chaos Mesh's setup cost first and fall back later.
- **Class imbalance** (normal windows vastly outnumber pre-failure windows) — plan for class-weighting or SMOTE from the start, report as an explicit methodological step, not an afterthought. Still open, applies at the ML-modeling phase (Weeks 8-9).
- **Some fault types don't reproduce realistically in a single-node Kind cluster** (e.g., true network partition needs multiple physical nodes) — RESOLVED 2026-07-08: partition leader churn dropped from the locked taxonomy for exactly this reason (see Section 6.2); the other 5 fault classes are all confirmed feasible single-node.
- **Someone publishes this exact intersection first** — the AIOps space is active (2025 payment-systems paper found in this pass alone). Re-checked 2026-07-08 (Weeks 2-3 saturation re-check, Section 3): gap still open, closest hit (DEBS 2024) answers a different question (recovery speed, not predictive lead time). Still an active risk — re-check again before the writing phase (Weeks 13-16).
- **Sample size per fault class revised down (N≥15-20 → N=8), 2026-07-11** — a reasoned call, not a shortcut: per-class coefficient-of-variation analysis on the completed N=8 campaign (see commit history, `fault_injection/campaign.py` checkpoint stats) showed 4 of 5 classes with CV in the 37-57% range and one (backpressure_cascade) at 12.9%, none showing the kind of instability that would demand doubling the repetition count outright. Risk being tracked, not eliminated: this is a judgment call from a single completed campaign, not a power analysis, and it could be wrong for a class that only reveals instability once ML modeling starts probing the tails of its distribution. Mitigation is reactive, not preventive — top up a specific class's N if modeling surfaces a problem there, rather than blanket-rescaling all 5 up front on a hunch. Re-evaluate this call explicitly if any class's held-out performance is anomalously poor relative to the others (Weeks 8-11).
- **False gate PASS on config-presence verification, 2026-07-11** — Criterion 5 (maxNumFailures fix) was recorded as PASS on 2026-07-11 (~15:16 UTC) based on config-presence verification only (reading the loaded spark.properties file). This was a false PASS -- the config key was invalid (spark.kubernetes.executor.maxNumFailures does not exist; correct key is spark.executor.maxNumFailures) and Spark silently ignored it. Caught 2026-07-11 (~16:34 UTC) when a 4th executor kill crossed the old default-3 boundary during unrelated ramp-calibration work. Lesson generalized: config-presence checks are insufficient for silently-ignored-flag risks; behavioral confirmation is required for any config claiming to change runtime failure-tolerance behavior. Remediated the same day: corrected key set in `infra/spark/submit-pod.yaml` (commit `3731a61`, 16:47:40 UTC), re-verified against a live driver's loaded properties file (`results/phase1-gate-evidence/live_maxnumfailures_check_CORRECTED.txt`), and behaviorally confirmed by 8 consecutive executor kills with 0 driver crashes across the redesigned executor_oom campaign (`ramptest3`-`ramptest10`).
- **backpressure_cascade dataset provenance gap from the same maxNumFailures bug, 2026-07-11** — the bug's scope was driver-lifetime, not local to executor_oom, so the other 4 classes' original collection manifests were cross-checked directly rather than trusting "none reported." `backpressure_cascade` rep1 failed at 10:49:32 UTC ("no running driver pod found"), immediately after executor_oom's class had already burned through 5 real executor kills past the true default ceiling of 3 -- plausible spillover, not provable after the fact (no driver log capture existed for that window). Its topup1 replacement (15:00:20 UTC) fell between the wrong-key "fix" and the real fix, so it was contaminated too. Both discarded and replaced with a fresh, post-fix rep (`refresh1`, 18:15:49 UTC) -- see `results/campaign-n8/_discarded/backpressure_cascade-unconfirmed-config/README.md`. The other 3 classes (broker_kill, disk_pressure, network_degradation) showed no equivalent failures in their original collection windows, so no further action was needed there.
- **executor_oom's Weeks 8-9 ML result does not clear chance at N=5 (or N=3 clean) episodes, 2026-07-12 — RESOLVED (confirmed negative) 2026-07-13.** gate-auditor caught this before it was reported as final: an earlier draft of Section 6.5 called executor_oom "positive, with caveats" on F1 alone; a null-baseline + permutation check (`executor_oom_null_baseline_check.json`, same rigor as the other 3 classes) found F1 below a trivial always-predict-pre_failure baseline in both configurations, and 34-54/100 shuffled F1≥real (p=0.34/0.54, chance). Not a data-quality bug like the other risk entries here — executor_oom's own extraction path is confirmed clean (see Section 6.5's bug #3 discussion) — at the time this was logged as a genuine small-N power problem: 5 episodes (3 after excluding cold-start-artifact ones) not enough to distinguish a real precursor pattern from noise, if one exists at all. **Mitigation executed 2026-07-13**: topped up to N=15 real episodes via the same reactive top-up path flagged here (`fault_injection/campaign.py --top-up-class executor_oom --top-up-target 15`, unchanged protocol), full pipeline re-run (`modeling/executor_oom_n15_reeval.py`, `executor_oom_n15_evaluation.json`). Result: 11/15 episodes usable, p=0.500 (full set) / p=0.520 (clean set) — chance, statistically indistinguishable from the N=8 result (p=0.48/0.56, corrected-config). **This closes the power question rather than reopening it**: doubling the real episode count did not move p out of chance territory, so the null is not an artifact of insufficient data. One residual structural finding, not a power problem: the clean (genuinely-settled-baseline) subset is capped at 2 episodes regardless of N, because this fault design's tight inter-episode gap means almost every executor pod is too young at onset for the normal-window definition (onset-150s to onset-90s) to find a real pre-onset sample — a fault-design constraint, not something more data fixes under the same protocol.

## 12. References — verification status

Confirmed via direct search in this research session:
- Notaro, P., Cardoso, J., Gerndt, M. (2021). *A Survey of AIOps Methods for Failure Management.* ACM Transactions on Intelligent Systems and Technology, 12(6), Article 81. DOI: 10.1145/3483424.
- Wu, Z., Xu, H., Pang, G., Yu, F., Wang, Y., Jian, S., Wang, Y. (2021). *DRAM Failure Prediction in AIOps: Empirical Evaluation, Challenges and Opportunities.* arXiv:2104.15052.

**Verify before citing** (topic/venue confirmed, full author list or exact detail not confirmed in this session — pull directly from arXiv/publisher page):
- *A Feature Engineering Approach for Business Impact-Oriented Failure Detection in Distributed Instant Payment Systems.* arXiv:2510.21710 (2025).
- *Bioinformatics Computational Cluster Batch Task Profiling with Machine Learning for Failure Prediction.* arXiv:1812.09537 (2018) — author names not fully confirmed in this pass.
- Gray-box modeling methodology for runtime prediction of Apache Spark jobs. *Distributed and Parallel Databases* (Springer, 2020) — pull full author/citation from Springer page directly.

**Saturation re-check log (2026-07-08, Weeks 2-3 gate per Section 8):** re-ran the searches specified at the top of this document (`"Kafka" "Spark Structured Streaming" failure prediction benchmark fault injection 2026`, `AIOps streaming pipeline fault injection lead time prediction 2026`, plus a targeted follow-up on ML lead-time prediction for Kafka/Spark specifically) before proceeding to fault-taxonomy lock. Gap confirmed still open. Closest hit (DEBS 2024 fault-recovery benchmark, added to the table above) answers a different question — recovery speed across competing frameworks, not predictive lead time on a fixed pipeline. No paper found combining controlled fault injection + multi-model ML + lead-time evaluation + threshold-baseline comparison for Kafka+Spark specifically. Proceeding on this basis.

---

## Immediate next actions

1. Re-run the saturation-check searches listed at the top of this document before starting the fault-injection campaign.
2. Pick the fault-injection tool (Chaos Mesh vs. manual scripts) given actual remaining time budget — don't default to the fancier tool if the manual path ships faster.
3. Run one pilot fault injection end-to-end (single broker kill) and confirm Prometheus actually captures a usable pre-failure signal window before committing to the full campaign — this is the single highest-leverage sanity check, do it first.
4. Pull exact BibTeX for every "verify before citing" reference above from the primary source.
5. Take this document to your guide framed as a working draft — front-matter fields and any guide-specific formatting still need to go in.
