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
Controlled, repeated injection of each fault class, with injection timestamp and observed failure/degradation-onset timestamp recorded as ground truth:
- Broker kill / restart (pod delete)
- Executor OOM-kill (memory pressure induction)
- Backpressure cascade (throttle downstream sink / slow consumer)
- Disk-pressure on broker (fill disk toward threshold)
- Network degradation (latency/packet-loss injection via `tc netem`, or Chaos Mesh if time permits)
- Partition leader churn (forced leader re-election)

Each fault repeated N times (target N≥15–20 per class for a usable sample) at randomized injection points during steady-state load, to avoid the model learning injection-schedule artifacts instead of genuine pre-failure signal.

### 6.3 Labeling
Sliding-window supervised framing, following the standard AIOps approach (Notaro et al., 2021): telemetry windows at multiple horizons before recorded failure onset (e.g., t-30s, t-60s, t-120s) are labeled "pre-failure"; windows during confirmed steady-state operation are labeled "normal." Window size and horizon are hyperparameters to sweep, not fixed a priori — report sensitivity.

### 6.4 Models and Baselines
- **ML models**: Random Forest, XGBoost/LightGBM on windowed statistical features (extends your existing Isolation Forest / DBSCAN anomaly-detection background into a supervised, lead-time-aware setting); optionally a lightweight temporal model (e.g., simple LSTM or GRU) as a stretch comparison if time allows.
- **Baseline (critical, non-negotiable)**: a static-threshold detector reproducing real alerting rules from current practice (consumer lag > X, under-replicated partitions > 0, executor memory > Y%) evaluated on the *same* fault-injection dataset. This is the baseline that makes the paper's claim testable — without it, "ML predicts failure" is an unfalsifiable claim.

### 6.5 Metrics
Precision, Recall, F1, AUROC for the classification framing; **lead time** (median seconds between first true-positive alert and actual recorded failure onset) as the headline metric; false-positive rate under confirmed normal operation (this is what determines whether the approach is usable in practice — a model that predicts constantly is worthless even with perfect recall).

### 6.6 Explainability
SHAP or permutation feature importance per fault class, to show *which* signals drive early warning for *which* failure type — this is the actionable-insight narrative for the write-up and for real operator adoption.

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
- DONE, verified: Spark Structured Streaming job (`infra/spark/`) consuming real messages from the Kafka topic via a continuous producer (`infra/kafka/producer-loadgen.yaml`), confirmed against driver logs showing real per-window event counts across 60+ micro-batches. Deployed via `spark-submit --deploy-mode cluster` directly against the K8s API (not the Spark Operator -- unnecessary extra surface for a single always-on job at this stage), using a pod template to work around a real Spark 4.1.2 limitation (`spark.kubernetes.driver.volumes.configMap.*` conf keys don't exist -- only hostPath/emptyDir/nfs/persistentVolumeClaim are supported that way; ConfigMap mounts require a pod template, confirmed against the official docs, not assumed).
- DONE, verified: Prometheus scraping Spark's built-in `spark.ui.prometheus.enabled` servlet (`/metrics/executors/prometheus/` on the driver, pod-discovery scrape job same pattern as Kafka) -- 47 distinct real `metrics_executor_*` series confirmed via Prometheus's own query API, not just target-up.
- DONE, verified: node exporter enabled (`prometheus-node-exporter.enabled=true`), confirmed via a real non-zero `node_memory_MemAvailable_bytes` value queried through Prometheus.
- Full Phase 0 deliverable list for this row is now complete. Known operational note (not a gate blocker): the pilot Structured Streaming job's 5s micro-batch trigger interval is undersized for this Kind node's resources -- observed batch durations of 16-27s, fluctuating rather than monotonically climbing (no OOM, no restarts across 20+ minutes). Worth widening the trigger interval before the real fault-injection campaign; not addressed now since it doesn't block this checkpoint.
- Mid-build incident: the Kind cluster was unexpectedly destroyed mid-session (Docker/WSL2 restart wiped the container; a sibling, unrelated Kind cluster on the same machine survived because it had a restart policy applied and ours didn't). No data was lost -- everything needed was already captured as code/evidence in git -- but the entire stack (Kind, Strimzi, Kafka, Prometheus, producer) had to be rebuilt from the committed IaC mid-session. This is itself a small piece of evidence for why infra-as-code + committed evidence (not just a running cluster) is the right discipline here.
| 2–3 | Saturation re-check + fault taxonomy lock + injection tooling | Confirmed gap still open; fault-injection scripts working end-to-end for at least one fault class |
| 4–5 | Full fault-injection campaign | Labeled dataset across all fault classes, N≥15–20 repetitions each, ground-truth timestamps recorded |
| 6–7 | Baseline implementation | Static-threshold detector reproducing real alerting rules, evaluated on the dataset |
| 8–9 | ML models | RF / XGBoost / LightGBM trained, tuned, window/horizon sensitivity swept |
| 10–11 | Lead-time evaluation | Full metric suite computed per fault class; ML vs. baseline comparison table |
| 12 | Explainability | SHAP analysis per fault class |
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

- **Fault-injection tooling friction** (Chaos Mesh setup complexity on Kind) — budget a dedicated week; fallback to manual `kubectl delete pod` / `tc netem` scripts if Chaos Mesh proves too heavy for the timeline.
- **Class imbalance** (normal windows vastly outnumber pre-failure windows) — plan for class-weighting or SMOTE from the start, report as an explicit methodological step, not an afterthought.
- **Some fault types don't reproduce realistically in a single-node Kind cluster** (e.g., true network partition needs multiple physical nodes) — prioritize fault types that Kind can genuinely emulate (broker kill, OOM, backpressure, disk pressure) over ones it can't; document the limitation rather than force it.
- **Someone publishes this exact intersection first** — the AIOps space is active (2025 payment-systems paper found in this pass alone). Mitigation: no infra head-start exists anymore (verified false, see Section 6.1) — this risk is now real, not hedged. Treat Phase 0 as urgent, not a formality.

## 12. References — verification status

Confirmed via direct search in this research session:
- Notaro, P., Cardoso, J., Gerndt, M. (2021). *A Survey of AIOps Methods for Failure Management.* ACM Transactions on Intelligent Systems and Technology, 12(6), Article 81. DOI: 10.1145/3483424.
- Wu, Z., Xu, H., Pang, G., Yu, F., Wang, Y., Jian, S., Wang, Y. (2021). *DRAM Failure Prediction in AIOps: Empirical Evaluation, Challenges and Opportunities.* arXiv:2104.15052.

**Verify before citing** (topic/venue confirmed, full author list or exact detail not confirmed in this session — pull directly from arXiv/publisher page):
- *A Feature Engineering Approach for Business Impact-Oriented Failure Detection in Distributed Instant Payment Systems.* arXiv:2510.21710 (2025).
- *Bioinformatics Computational Cluster Batch Task Profiling with Machine Learning for Failure Prediction.* arXiv:1812.09537 (2018) — author names not fully confirmed in this pass.
- Gray-box modeling methodology for runtime prediction of Apache Spark jobs. *Distributed and Parallel Databases* (Springer, 2020) — pull full author/citation from Springer page directly.

---

## Immediate next actions

1. Re-run the saturation-check searches listed at the top of this document before starting the fault-injection campaign.
2. Pick the fault-injection tool (Chaos Mesh vs. manual scripts) given actual remaining time budget — don't default to the fancier tool if the manual path ships faster.
3. Run one pilot fault injection end-to-end (single broker kill) and confirm Prometheus actually captures a usable pre-failure signal window before committing to the full campaign — this is the single highest-leverage sanity check, do it first.
4. Pull exact BibTeX for every "verify before citing" reference above from the primary source.
5. Take this document to your guide framed as a working draft — front-matter fields and any guide-specific formatting still need to go in.
