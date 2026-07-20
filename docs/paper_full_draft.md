<!--
CONTENT-COMPLETE DRAFT (Markdown) — Stage 1 of 2 per author instruction 2026-07-20.
Target: IEEE two-column conference format, 6 pages preferred, 7 hard maximum.
Stage 2 (port to IEEE-Template/conference_101719.tex) happens ONLY after this draft is locked.

EVERY number traces to a committed source (docs/paper_draft.md Results, docs/paper_tables.md,
docs/baseline_thresholds.md, results/ml-first-pass/*.json, results/DATASET.md). No number or
citation is invented. Citations are numbered in order of first appearance; the reference list
at the bottom matches that order. All 19 references are real and were verified this project
(literature/references.bib, literature/LITERATURE_MATRIX.md).

Figure/table CUT LIST for the 6-7 page budget (from 11 available floats down to 7):
  KEEP: Table I (fault taxonomy), Table II (dataset), Table III (related-work grid),
        Table IV (main results), Fig 1 (architecture), Fig 2 (SHAP), Fig 3 (baseline lead time).
  FOLD INTO PROSE (numbers stated, no float): multi-model table, N8-vs-N15 figure, ramp figure.
-->

# Title

**Primary:** A Fault-Injection Benchmark for Lead-Time Failure Prediction on Kafka–Spark Structured Streaming Pipelines

<!-- Alternative, finding-forward (author's choice — see note): "Machine Learning Does Not Beat
Static Thresholds for Kafka–Spark Pipeline Failure Prediction: A Fault-Injection Benchmark" -->

**Authors**

1st Renold Stephen R — Dept. of Computer Science and Engineering, Christ University, Bangalore, India — renold.stephen@mtech.christuniversity.in
2nd H. Karthikeyan — Dept. of Computer Science and Engineering, Christ University, Bangalore, India — karthikeyan.h@christuniversity.in
3rd Diana Jeba Jingle I — Dept. of Computer Science and Engineering, Christ University, Bangalore, India — diana.jebajingle@christuniversity.in

---

## Abstract

Kafka and Spark Structured Streaming pipelines are core data infrastructure, and their failures — broker crashes, executor out-of-memory kills, disk exhaustion, network degradation — are caught in current practice by static-threshold alerting that fires only after degradation is visible. Whether supervised machine learning on pre-failure telemetry can warn earlier, and by how much, has not been benchmarked for this pipeline layer specifically. We build a reproducible fault-injection benchmark on a live Kafka + Spark Structured Streaming pipeline instrumented with Prometheus, physically inject five fault classes with onset timestamps recorded by the injection tooling itself, and construct a labeled dataset of 47 fault episodes (102 telemetry windows). We compare three tree-based classifiers (Random Forest, XGBoost, LightGBM) under leave-one-group-out cross-validation against a static-threshold baseline that reproduces real alerting practice, using rank-based permutation testing and a lead-time metric. Only one of five fault classes (disk_pressure) produces a classification signal that clears chance-level significance (F1 = 0.941, p < 0.01); per-instance ablation and SHAP show even this is an absolute-value discriminator, not detection of a physical precursor. No class yields a machine-learning lead-time advantage over the threshold baseline, a null result that holds across all three classifiers and does not move when one class is enlarged from 8 to 15 episodes. The contribution is the open benchmark, the labeled dataset, and a mechanism-verification methodology that catches such over-claims before they are reported.

**Keywords:** AIOps, fault injection, failure prediction, Apache Kafka, Spark Structured Streaming, lead time, benchmark, static-threshold alerting

---

## I. Introduction

Streaming data infrastructure — Apache Kafka [1] for ingestion and Spark Structured Streaming [2] for processing — underpins modern data platforms, and its reliability is operationally critical: a stalled consumer group or a crashed broker degrades downstream data freshness long before an operator notices. The dominant operational response is threshold-based alerting: fire when consumer lag exceeds a bound, or when under-replicated partitions rise above zero. This is reactive by construction — the threshold is crossed only once degradation is already under way.

Proactive failure prediction has been the alternative premise for over a decade [3]. The AI-for-IT-operations (AIOps) literature has established, across general distributed-systems settings, that machine-learning (ML) models trained on system telemetry can anticipate failures earlier than static rules [4], [5]. This has been shown concretely in adjacent domains: DRAM failure prediction at data-center scale [6], node-failure prediction in cloud service systems [7], and batch-cluster job-failure prediction [8]. One recent system predicts both failure and its lead time, though on high-performance-computing (HPC) logs rather than streaming telemetry [9]. What has not been benchmarked with the same rigor is the specific combination of controlled fault injection, multi-model comparison, and lead-time evaluation applied to a Kafka + Spark Structured Streaming pipeline as the object of prediction — as distinct from anomaly detection on the data flowing *through* the pipeline, and from Spark job runtime prediction, both of which are separately studied.

This paper reports that benchmark, and its result is a cautionary one. Across five physically injected fault classes, only one produces a classification signal that survives a permutation test against chance, and closer inspection shows that signal is not the early-warning capability it superficially resembles. No fault class yields a machine-learning lead-time advantage over a static-threshold baseline that reproduces current practice. We report this negative result with the same rigor as a positive one, because the value here is the measurement apparatus and what it honestly finds, not a model that wins.

**Contributions.**

- A reproducible, open fault-injection benchmark on a live Kafka + Spark Structured Streaming pipeline, with infrastructure-as-code, five injection scripts, and a static-threshold baseline reproducing current alerting practice.
- A labeled dataset of 47 real fault episodes across five classes, each carrying an onset timestamp recorded by the injection tooling at the moment it caused the fault — not inferred or human-annotated afterward — with paired Prometheus telemetry that makes lead-time computation possible.
- A lead-time evaluation and multi-model comparison (Random Forest, XGBoost, LightGBM) against the threshold baseline, showing no ML lead-time advantage on this pipeline for any class.
- A mechanism-verification methodology — per-instance ablation and SHAP cross-checking applied to every positive-looking result before it is trusted — which caught this study's one significant classification result as an absolute-value discriminator rather than genuine precursor detection.

No contribution here is a novel model or algorithm; every classifier is a stock implementation with standard hyperparameters. The contribution is the benchmark, the dataset, and the measurement discipline.

The remainder of the paper is organized as follows. Section II positions the work against prior art. Section III describes the infrastructure, fault taxonomy, dataset, baseline, and evaluation. Section IV reports results, Section V discusses them against the research questions and prior work, and Section VI concludes.

---

## II. Related Work

**Failure prediction in distributed systems.** Online failure prediction — using runtime telemetry to flag impending failure — has a well-developed taxonomy [3], and AIOps surveys establish ML-over-thresholds as a mature premise across IT operations [4], [5]. Concrete demonstrations exist in adjacent domains: an empirical multi-classifier benchmark for DRAM failures [6], a Random-Forest job-failure predictor on HPC batch clusters reaching 95.4% accuracy [8], and MING, which predicts node failures in Microsoft cloud systems at 63.5% recall and 92.4% precision [7]. These establish that the method works — but in hardware, batch-scheduling, and node-level settings, none on a streaming-pipeline layer, and none reporting a lead-time metric except in HPC logs.

**Lead-time prediction.** The closest thematic neighbor is Time Machine [9], which predicts failure, its location, and its lead time using a transformer-decoder over HPC event logs. It shares our target — lead time as an explicit prediction quantity — but differs on every axis that matters here: it learns from naturally occurring failures rather than controlled injection, operates on log events rather than Prometheus telemetry, compares against deep-learning baselines rather than a static-threshold detector, and contributes a model rather than a benchmark.

**Fault injection and chaos engineering.** Deliberately inducing faults to study system behavior is an established experimental discipline [10], surveyed comprehensively for software fault injection [11]. Recent work injects node-, pod-, and network-level faults into live Kubernetes clusters and measures application-level resilience [12]. Our injection methodology sits in this lineage, but where [12] measures post-fault resilience and stability, we convert injection output into a labeled dataset for predictive modeling.

**Streaming-pipeline reliability benchmarks.** The nearest systems benchmark injects chaos-style faults across Flink, Kafka Streams, and Spark Structured Streaming and compares fault-recovery performance [13]. It shares our substrate — Kafka and Spark on Kubernetes with injected faults — but answers a different question: post-hoc recovery speed across competing engines, not predictive lead time on a fixed pipeline. It reports no labeled pre-failure dataset and no static-threshold-versus-ML comparison. Separately, gray-box modeling predicts Spark job *runtime* [14]; this sharpens rather than competes with our framing — predicting execution time is a distinct, solved problem from predicting failure. The open-benchmark-plus-reproducible-baselines pattern we follow is exemplified in microservice root-cause analysis by RCAEval [15], though that addresses post-hoc diagnosis rather than prediction.

**Positioning.** No located work combines controlled fault injection, multi-model ML, lead-time evaluation, and a static-threshold baseline on a fixed Kafka + Spark Structured Streaming pipeline. Table III makes the gap explicit: each prior work satisfies some of these criteria, none satisfies all, and it is their conjunction on this pipeline layer that this benchmark occupies.

---

## III. Methodology

### A. System Under Test and Infrastructure

The pipeline runs on a single-node Kind Kubernetes cluster: a single Strimzi-managed Kafka broker (Kafka 4.3.0) with a JMX exporter, a Spark 4.1.2 Structured Streaming job (driver plus executors) consuming from Kafka, and a continuous producer generating load. Prometheus scrapes three metric sources on a 60-second interval — the broker's JMX exporter, cAdvisor container metrics, and node-exporter host metrics. Fig. 1 shows the topology and the five fault-injection points. The entire stack is declarative infrastructure-as-code (`kubectl apply`), reproducible from the repository without a running cluster; the injection scripts operate against this live deployment. A single-node cluster is a deliberate scope choice — it makes the benchmark reproducible on a laptop-class machine, at the cost of the external-validity limitation discussed in Section V.

### B. Fault Taxonomy and Injection

Five fault classes are injected, chosen to span Kafka-side, Spark-side, and node-level failure modes reproducible on a single node (Table I). Each is a real, physical fault: broker_kill deletes the broker pod; executor_oom drives a Spark executor's memory up a gradual ramp until the Kubernetes OOM killer fires; disk_pressure fills the node filesystem; network_degradation applies `tc netem` delay, jitter, and loss to the broker; backpressure_cascade bursts producer load to induce consumer lag. Each injection script records its own ground-truth timestamps — injection onset and a class-specific confirmation event (e.g., `oomkilled_confirmed_utc`, `drop_confirmed_utc`) — at the moment it acts, so labels derive from the tooling's own record rather than post-hoc inference.

The static-threshold rule for each class is grounded in this pipeline's own observed telemetry, not borrowed from an industry rule of thumb: thresholds sit with real margin between the measured baseline and the measured fault-window value (Table I). backpressure_cascade is injected and its ground truth recorded, but excluded from modeling: no independently scraped Prometheus metric reflects Spark processing lag on this deployment — the only available lag field is the driver-log-parsed one the ground truth itself uses, so any detector built from it would be circular.

### C. Dataset Construction and Labeling

The campaign yields 47 active fault episodes and 20 discarded ones, every exclusion carrying a per-episode recorded reason (Table II). Feature extraction produces 102 labeled telemetry windows across the four modeled classes. For each window, six raw statistics (mean, standard deviation, min, max, last value, sample count) are computed over the metric most relevant to that fault class; delta features expressing each statistic relative to the episode's own pre-injection baseline are derived on top. Pre-failure windows are drawn from the horizon immediately before each episode's recorded onset; normal windows come either from a genuinely quiet reference period or, for the pod-scoped executor_oom class, from each episode's pre-injection settling period. The window/horizon configuration (15-second windows ending 10–15 seconds before onset) was selected after a four-point sensitivity sweep showed wider configurations were not contamination-safe at the tightest real inter-episode gaps.

### D. Static-Threshold Baseline

The baseline reproduces current alerting practice: a detector fires when the class's metric crosses its threshold (Table I). It is evaluated on the same episodes as the models — detection recall and lead time on the 8 active reps per class, and false-positive rate on the deliberate inter-episode steady-state gaps. Thresholds are calibrated against these same episodes' observed values; this is standard for a baseline meant to mirror how an operations team tunes its own alerts, but it means the baseline's recall reflects fit to its calibration data, not out-of-sample generalization — stated plainly as a limitation rather than hidden.

### E. Models, Evaluation, and Significance Testing

Three stock tree-based classifiers are compared: Random Forest [16], XGBoost [17], and LightGBM [18]. The reported configuration is `RandomForestClassifier(n_estimators=200, max_depth=5, class_weight="balanced")`; class weighting addresses the pre-failure/normal imbalance. Because episodes are the unit of independence, all evaluation uses leave-one-group-out (LOO) cross-validation grouped by episode, so no window from a test episode appears in training. Two guards protect against over-reading. First, every result is tested for significance by a rank-based permutation test: the identical pipeline is re-run on 100 label-shuffled datasets, and the p-value is the fraction scoring an F1 at or above the real result. Second, any positive-looking result is subjected to mechanism verification — per-instance ablation to locate the classifier's actual decision boundary, and SHapley Additive exPlanations (SHAP) [19] attribution over the real cross-validation folds — before it is reported as evidence of early detection. Lead time, where computable, is measured by a contamination-safe backward scan from injection onset, bounded by the real measured gap to the nearest preceding episode of any class.

---

## IV. Results

Table IV summarizes classification and lead-time outcomes across the five fault classes. Two results need methodological unpacking before they can be read at face value, and both are addressed below.

### A. Classification Performance Across Fault Classes

Of the five classes, only disk_pressure clears permutation-test significance. broker_kill's full feature set produced an uncorrected F1 of 0.667, but feature-importance inspection found the sample-count feature carrying 100% of the model's importance — the availability metric this class scrapes is flat regardless of window position, so only the incidental sample-count parity between pre-failure and normal windows was driving classification. Removing that feature drops the score to F1 = 0.762 (precision 0.615, recall 1.000), which the permutation test places at chance (p = 0.25). This null is not a weak-model artifact: at the pipeline's 60-second scrape interval, the class's own ground truth captured an in-fault telemetry sample in only 1 of 8 episodes, so most labeled pre-failure windows have no observable signal to learn from. network_degradation is negative under the full feature set (F1 = 0.414); a shape-only (standard-deviation) ablation returns F1 = 0.118, below all 100 shuffled reruns (p = 1.00), the cleanest negative in the study. Model choice does not change any verdict: Random Forest, XGBoost, and LightGBM agree per class under nested-tuned LOO-CV.

### B. disk_pressure: A Significant but Mischaracterized Signal

disk_pressure is the only class clearing significance: F1 = 0.941 (precision 0.889, recall 1.000) using delta features, with 0 of 100 shuffled reruns matching the real F1 (p < 0.01). A result this clean warranted checking *what* the classifier had learned. Per-instance ablation swept each held-out fold's delta feature to the value at which its prediction flips; all seven ablated folds located this boundary at the same value, −350,000 bytes, with zero variation — the signature of a static absolute-value check, not a graded response to an escalating quantity. A SHAP analysis of the same LOO models corroborates this independently: across all 16 true-positive windows, the four magnitude features carry 97.4% of mean absolute attribution (Fig. 2). The raw delta values at the classifier's trained decision horizons are uniformly non-negative — available disk space had not begun declining at any window the classifier saw — so there was no downward trend for any feature to detect. The classifier distinguishes "disk_pressure recording session" from "quiet reference period" on an absolute-value basis; that distinction is real and statistically significant, but it is not detection of the injected fault's physical precursor, whose true magnitude (a 3.22 GB drop) is three to four orders of magnitude larger than the ≈350 KB margin the classifier relies on.

### C. Lead-Time Evaluation

The central research question is whether models predict failure with a meaningful lead time relative to the threshold baseline. Table IV's baseline column shows where static thresholds themselves give warning. Only executor_oom's deliberately engineered gradual ramp yields a consistent lead: the threshold crosses a mean 64.9 seconds before the OOM kill (range 48–83 s across 8 reps), because the ramp produces real intermediate samples (Fig. 3). disk_pressure and network_degradation collapse to ≈0 s — their telemetry jumps from baseline to full fault magnitude in a single 60-second scrape step, leaving no intermediate sample; network_degradation additionally reaches its severity bar in only 4 of 8 reps. broker_kill's baseline is not a lead time at all: for a binary up/down signal, threshold crossing coincides with the outage, and detection recall is 12.5% at this scrape interval.

On the ML side, no class supports a positive lead-time claim. Three classes (broker_kill, network_degradation, executor_oom) produced no significant classification signal, so no lead time was computed — reporting one for a chance-level classifier would misrepresent it. disk_pressure is the only class where a lead time could be computed (mean 98.6 s over 7 of 8 episodes, 55–110 s), but Section IV-B shows this figure describes how far back the absolute-value discriminator remains detectable within the scan window, not a physical detection horizon. The eighth episode returned a positive prediction at every point tested and is reported as an unresolved artifact, excluded from the mean rather than folded in to make a cleaner number. Zero of five classes support a machine-learning lead-time advantage over the baseline.

### D. Sample-Size Robustness

executor_oom's negative result at N = 8 raised whether eight episodes were simply too few. The collection was enlarged to 15 episodes under the identical protocol and re-evaluated. Of 15 raw episodes, 11 yielded a usable window. F1 rose to 0.909, but the trivial always-predict-pre-failure baseline rose with it to 0.957 — the classifier still does not beat a constant prediction — and the permutation test placed the result at p = 0.500. Doubling the episode count moved neither the p-value (0.500 vs. 0.48 at N = 8) nor the beats-trivial-baseline verdict; the null is not an artifact of insufficient statistical power. A genuinely settled pre-onset window was available for only two episodes, a structural consequence of the gradual-ramp protocol (each executor pod is 30–90 seconds old at the next episode's onset), not a sampling shortfall further collection would fix.

---

## V. Discussion

### A. Answering the Research Questions

The study asked four questions. Can models predict failure with a meaningful lead time (RQ1)? For this pipeline, no — no class produced a lead-time advantage. Which signals are most predictive, and does it vary by class (RQ2)? Only disk_pressure yielded a signal to attribute, and SHAP showed it rests on absolute magnitude rather than any precursor dynamic — so the honest answer is that no class exposed a genuinely predictive precursor signal in this telemetry. How does prediction quality vary across fault types (RQ3)? It is uniformly at chance except for disk_pressure's mischaracterized case. Does ML outperform static-threshold alerting (RQ4)? Not on this pipeline: the one class where a static threshold gives real warning (executor_oom, 64.9 s) is a class where the model is at chance, and the model's one significant result does not translate into operational lead time.

### B. Why a Null Result Here Is a Contribution

That ML beats static thresholds is close to a foregone conclusion in the AIOps literature [4], [6], [7], and a paper that simply confirmed it would add little. The value of this result is the opposite: on a realistic, fully labeled Kafka–Spark pipeline, with ground-truth onset timestamps and honest significance testing, the expected advantage does not appear — and the one result that superficially shows it dissolves under mechanism verification. Two structural findings explain why. First, at a production-typical 60-second scrape interval, three of five fault classes progress from baseline to failure inside a single scrape step, leaving no intermediate telemetry for either a threshold or a model to act on; lead-time prediction is bounded by scrape cadence before it is bounded by model capacity. Second, only a deliberately engineered gradual ramp (executor_oom) creates a precursor window at all, and even there the static threshold already captures it — the model adds nothing. These are properties of the telemetry and the faults, and they will hold for any predictor, which is why reporting them matters.

### C. Positioning Against Closest Prior Work

The result does not contradict Time Machine's lead-time prediction [9]: that system works on dense HPC event logs with far finer temporal resolution than 60-second Prometheus scrapes, and on naturally occurring failures rather than injected ones. It is consistent with, and extends, the recovery-focused streaming benchmark of Vogel et al. [13] — where they show engine recovery behavior differs under injected faults, we show that the *predictive* signal ahead of those faults is largely absent at standard scrape cadence. The mechanism-verification step is the methodological transfer we most want to see reused: none of the adjacent ML-failure-prediction studies [6], [7], [8] reports per-instance ablation or SHAP cross-checking of *why* a positive result holds, and had we not applied it, disk_pressure's F1 = 0.941 would have been reported as a 98.6-second early-warning success.

### D. Limitations

Three limitations bound the claims. The deployment is a single-node Kind cluster; behavior at production scale, with multiple brokers and executors and higher scrape resolution, is untested and could differ — particularly the scrape-cadence bound, which finer monitoring would relax. Sample sizes are small (8–15 episodes per class); the executor_oom enlargement showed the null is stable, but other classes were not similarly enlarged. The static-threshold baseline is calibrated on the same episodes it is evaluated on, so its recall reflects fit rather than generalization — appropriate for reproducing an operator's own-telemetry tuning, but not an out-of-sample claim. Each is a concrete target for replication rather than a reason to discount the benchmark.

---

## VI. Conclusion

We built an open, reproducible fault-injection benchmark for failure prediction on a Kafka + Spark Structured Streaming pipeline, comprising infrastructure-as-code, five physically injected fault classes, a labeled dataset of 47 episodes with tooling-recorded onset timestamps, and a static-threshold baseline reproducing current alerting practice. Across three stock classifiers evaluated under leave-one-group-out cross-validation with permutation testing, only one of five fault classes produced a statistically significant classification signal, and mechanism verification showed even that signal to be an absolute-value discriminator rather than genuine precursor detection. No class yielded a machine-learning lead-time advantage over the threshold baseline, a null result stable across classifiers and across a doubling of one class's sample size. Two structural causes — the 60-second scrape cadence bounding available precursor telemetry, and only engineered gradual faults creating a precursor window a threshold already captures — explain the result and will constrain any predictor on similar telemetry. The benchmark, dataset, and the mechanism-verification methodology that caught this study's lone over-claim are released as reusable artifacts. Future work should relax the scrape-cadence bound with finer monitoring, replicate at multi-node production scale, and recover the excluded backpressure_cascade class by adding independent Spark-lag instrumentation.

---

## References

<!-- IEEE numbered, in order of first appearance. Metadata from literature/references.bib (verified).
     Journal/proceedings titles italicized when ported to LaTeX. TODO-verify items flagged inline. -->

[1] J. Kreps, N. Narkhede, and J. Rao, "Kafka: a distributed messaging system for log processing," in *Proc. NetDB*, 2011.

[2] M. Armbrust, T. Das, J. Torres, B. Yavuz, S. Zhu, R. Xin, A. Ghodsi, I. Stoica, and M. Zaharia, "Structured Streaming: a declarative API for real-time applications in Apache Spark," in *Proc. ACM SIGMOD Int. Conf. Manag. Data*, 2018, pp. 601–613, doi: 10.1145/3183713.3190664.

[3] F. Salfner, M. Lenk, and M. Malek, "A survey of online failure prediction methods," *ACM Comput. Surv.*, vol. 42, no. 3, pp. 1–42, 2010, doi: 10.1145/1670679.1670680.

[4] P. Notaro, J. Cardoso, and M. Gerndt, "A survey of AIOps methods for failure management," *ACM Trans. Intell. Syst. Technol.*, vol. 12, no. 6, art. 81, 2021, doi: 10.1145/3483424.

[5] L. Zhang, T. Jia, M. Jia, Y. Wu, A. Liu, Y. Yang, Z. Wu, X. Hu, P. S. Yu, and Y. Li, "A survey of AIOps for failure management in the era of large language models," *ACM Comput. Surv.*, 2024, doi: 10.1145/3746635.

[6] Z. Wu, H. Xu, G. Pang, F. Yu, Y. Wang, S. Jian, and Y. Wang, "DRAM failure prediction in AIOps: empirical evaluation, challenges and opportunities," *arXiv:2104.15052*, 2021.

[7] Q. Lin, K. Hsieh, Y. Dang, H. Zhang, K. Sui, Y. Xu, J.-G. Lou, C. Li, Y. Wu, R. Yao, M. Chintalapati, and D. Zhang, "Predicting node failure in cloud service systems," in *Proc. ACM ESEC/FSE*, 2018, pp. 480–490, doi: 10.1145/3236024.3236060.

[8] C. Harrison, C. R. Kirkpatrick, and I. Dutra, "Bioinformatics computational cluster batch task profiling with machine learning for failure prediction," *arXiv:1812.09537*, 2018.

[9] K. A. Alharthi, A. Jhumka, S. Di, L. Gui, F. Cappello, and S. McIntosh-Smith, "Time Machine: generative real-time model for failure (and lead time) prediction in HPC systems," in *Proc. 53rd IEEE/IFIP Int. Conf. Dependable Syst. Netw. (DSN)*, 2023, pp. 508–521, doi: 10.1109/DSN58367.2023.00054.

[10] A. Basiri, N. Behnam, R. de Rooij, L. Hochstein, L. Kosewski, J. Reynolds, and C. Rosenthal, "Chaos engineering," *IEEE Softw.*, vol. 33, no. 3, pp. 35–41, 2016, doi: 10.1109/MS.2016.60.

[11] R. Natella, D. Cotroneo, and H. Madeira, "Assessing dependability with software fault injection: a survey," *ACM Comput. Surv.*, vol. 48, no. 3, art. 44, 2016, doi: 10.1145/2841425.

[12] Z. Chen, M. Goudarzi, and A. Nadjaran Toosi, "Resilience evaluation of Kubernetes in cloud-edge environments via failure injection," *arXiv:2507.16109*, 2025.

[13] A. Vogel, S. Henning, E. Perez-Wohlfeil, O. Ertl, and R. Rabiser, "A comprehensive benchmarking analysis of fault recovery in stream processing frameworks," in *Proc. 18th ACM Int. Conf. Distrib. Event-Based Syst. (DEBS)*, 2024, doi: 10.1145/3629104.3666040.

[14] H. Al-Sayeh, S. Hagedorn, and K.-U. Sattler, "A gray-box modeling methodology for runtime prediction of Apache Spark jobs," *Distrib. Parallel Databases*, vol. 38, pp. 819–839, 2020, doi: 10.1007/s10619-020-07286-y.

[15] L. Pham, H. Zhang, H. Ha, F. Salim, and X. Zhang, "RCAEval: a benchmark for root cause analysis of microservice systems with telemetry data," in *Companion Proc. ACM Web Conf. (WWW '25 Companion)*, 2025, pp. 777–780, doi: 10.1145/3701716.3715290.

[16] L. Breiman, "Random forests," *Mach. Learn.*, vol. 45, no. 1, pp. 5–32, 2001, doi: 10.1023/A:1010933404324.

[17] T. Chen and C. Guestrin, "XGBoost: a scalable tree boosting system," in *Proc. 22nd ACM SIGKDD Int. Conf. Knowl. Discov. Data Min. (KDD)*, 2016, pp. 785–794, doi: 10.1145/2939672.2939785.

[18] G. Ke, Q. Meng, T. Finley, T. Wang, W. Chen, W. Ma, Q. Ye, and T.-Y. Liu, "LightGBM: a highly efficient gradient boosting decision tree," in *Adv. Neural Inf. Process. Syst. (NeurIPS)*, 2017, pp. 3146–3154.

[19] S. M. Lundberg and S.-I. Lee, "A unified approach to interpreting model predictions," in *Adv. Neural Inf. Process. Syst. (NeurIPS)*, 2017, pp. 4765–4774.

---

<!-- FLOAT DEFINITIONS (rendered in LaTeX at port time; captions below figs, above tables per IEEE) -->

**Table I.** Fault taxonomy: the five injected classes, their injection mechanism, target, monitored Prometheus signal, and static-threshold rule. Source: `docs/baseline_thresholds.md`.

**Table II.** Dataset summary: active/discarded episode counts per class with discard reasons, ground-truth confirmation field, and modeling window counts (47 active, 20 discarded, 102 windows). Source: `results/DATASET.md`.

**Table III.** Related-work positioning grid: six criteria (controlled fault injection, ML failure prediction, lead-time evaluation, static-threshold baseline, Kafka+Spark Structured Streaming, open labeled dataset) against prior work; only this work satisfies all six. Source: `literature/LITERATURE_MATRIX.md`.

**Table IV.** Classification and lead-time outcomes across the five fault classes: LOO-CV F1 (class-specific corrected feature set), rank-based permutation p (100 shuffles), static-threshold baseline lead time, ML lead time, and verdict. Source: `docs/paper_draft.md` §3.1.

**Fig. 1.** System under test — single-node Kind cluster with Strimzi Kafka, Spark Structured Streaming, and Prometheus (60 s scrape) — and the five fault-injection points. File: `results/figures/fig2_architecture.pdf`.

**Fig. 2.** SHAP attribution for the disk_pressure delta-feature classifier (F1 = 0.941, p < 0.01) across all 16 true-positive windows: the four magnitude features carry 97.4% of attribution, the fingerprint of an absolute-value discriminator rather than trend detection. File: `results/ml-first-pass/disk_pressure_shap_summary.png`.

**Fig. 3.** Static-threshold warning time by fault class: only executor_oom's engineered ramp yields a real lead window (mean 64.9 s); disk_pressure is 0 s (8/8), network_degradation 0 s×3 / 65 s×1 (4/8 reach severity), broker_kill N/A. File: `results/figures/fig4_baseline_leadtime.pdf`.
