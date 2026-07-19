# KSPFail — Literature Collection

Curated, **verified** literature for the KSPFail dissertation: *ML-based failure and
lead-time prediction on a Kafka + Spark Structured Streaming pipeline, driven by controlled
fault injection, benchmarked against static thresholds, with SHAP-based mechanism checking.*

**20 papers**, grouped into 5 themes. Each was confirmed against its primary source
(arXiv abstract page, DOI landing page, or publisher record) — author list, year, venue,
and identifier read directly off the source, never from memory or a title guess.

---

## How authenticity was checked (read this — it matters)

There is **no "research-paper auditor" skill** installed in this project. The only skills
present are `research-mentor`, `paper-writer`, and `run-kspfail` — none audits citation
authenticity. Rather than fake an audit, every paper here was verified by the same discipline
that caught two real citation errors earlier in this project:

1. **Resolve the identifier** (arXiv ID / DOI) against its own landing page.
2. **Read the author list, year, and venue directly off that page** — not from the title, not from memory.
3. **Download the full text where openly available**, and for PDFs pulled from proceedings/repository
   hosts, confirm the embedded `/Title` + `/Author` metadata matches (done for LightGBM, gray-box Spark,
   Structured Streaming).

Corrections this pass made vs. what the project's own `docs/research_context.md` had recorded:

- **Time Machine** was logged as "IEEE, June 2023, document 10202658" with **no authors**. It is actually
  **DSN 2023** (IEEE/IFIP Dependable Systems & Networks), DOI 10.1109/DSN58367.2023.00054, authors
  Alharthi, Jhumka, Di, Gui, Cappello, McIntosh-Smith.
- **DEBS 2024 benchmark** had **no author list** recorded — now Vogel, Henning, Perez-Wohlfeil, Ertl, Rabiser.
- **Gray-box Spark paper** was flagged "verify before citing" — resolved: Al-Sayeh, Hagedorn, Sattler,
  *Distributed and Parallel Databases* 38 (2020), DOI 10.1007/s10619-020-07286-y.

If you cite any of these, the metadata below is safe to use as-is. For the 3 metadata-only entries,
pull the PDF through your institutional (Christ University) ACM access before final submission.

---

## Full inventory

| # | Theme | Citation (short) | Year | Identifier | PDF |
|---|-------|------------------|------|------------|-----|
| 1 | Surveys | Notaro, Cardoso, Gerndt — *A Survey of AIOps Methods for Failure Management* | 2021 | DOI 10.1145/3483424 | metadata-only |
| 2 | Surveys | Zhang et al. — *A Survey of AIOps for Failure Management in the Era of LLMs* | 2024 | arXiv:2406.11213 · DOI 10.1145/3746635 | ✅ |
| 3 | Surveys | Salfner, Lenk, Malek — *A Survey of Online Failure Prediction Methods* | 2010 | DOI 10.1145/1670679.1670680 | metadata-only |
| 4 | ML failure pred. | Wu et al. — *DRAM Failure Prediction in AIOps* | 2021 | arXiv:2104.15052 | ✅ |
| 5 | ML failure pred. | Porcelli — *Failure Detection in Distributed Instant Payment Systems* | 2025 | arXiv:2510.21710 | ✅ |
| 6 | ML failure pred. | Harrison, Kirkpatrick, Dutra — *Batch Task Profiling with ML for Failure Prediction* | 2018 | arXiv:1812.09537 | ✅ |
| 7 | ML failure pred. | Alharthi et al. — *Time Machine: Failure (and Lead Time) Prediction in HPC* | 2023 | DOI 10.1109/DSN58367.2023.00054 | ✅ |
| 8 | ML failure pred. | Lin et al. — *Predicting Node Failure in Cloud Service Systems* | 2018 | DOI 10.1145/3236024.3236060 | ✅ |
| 9 | ML failure pred. | Pham et al. — *RCAEval: Root Cause Analysis Benchmark with Telemetry Data* | 2025 | WWW'25 Companion · DOI 10.1145/3701716.3715290 · arXiv:2412.17015 | ✅ |
| 10 | Fault injection | Basiri et al. — *Chaos Engineering* | 2016 | DOI 10.1109/MS.2016.60 · arXiv:1702.05843 | ✅ |
| 11 | Fault injection | Natella, Cotroneo, Madeira — *Assessing Dependability with Software Fault Injection* | 2016 | DOI 10.1145/2841425 | metadata-only |
| 12 | Fault injection | Chen, Goudarzi, Nadjaran Toosi — *Resilience Evaluation of Kubernetes via Failure Injection* | 2025 | arXiv:2507.16109 | ✅ |
| 13 | Stream proc. | Vogel et al. — *Benchmarking Fault Recovery in Stream Processing Frameworks* (DEBS'24) | 2024 | arXiv:2404.06203 | ✅ |
| 14 | Stream proc. | Al-Sayeh, Hagedorn, Sattler — *Gray-box Runtime Prediction of Apache Spark Jobs* | 2020 | DOI 10.1007/s10619-020-07286-y | ✅ |
| 15 | Stream proc. | Kreps, Narkhede, Rao — *Kafka: a Distributed Messaging System for Log Processing* | 2011 | NetDB'11 | ✅ |
| 16 | Stream proc. | Armbrust et al. — *Structured Streaming (Apache Spark)* | 2018 | DOI 10.1145/3183713.3190664 | ✅ |
| 17 | Methods/XAI | Breiman — *Random Forests* | 2001 | DOI 10.1023/A:1010933404324 | ✅ |
| 18 | Methods/XAI | Chen, Guestrin — *XGBoost: A Scalable Tree Boosting System* | 2016 | DOI 10.1145/2939672.2939785 · arXiv:1603.02754 | ✅ |
| 19 | Methods/XAI | Ke et al. — *LightGBM* | 2017 | NeurIPS 2017 | ✅ |
| 20 | Methods/XAI | Lundberg, Lee — *SHAP: A Unified Approach to Interpreting Model Predictions* | 2017 | NeurIPS 2017 · arXiv:1705.07874 | ✅ |

**17 full-text PDFs · 3 metadata-only.** BibTeX for all 20: [`references.bib`](references.bib).

**Recency (2021–2026), 8 of 20:** #2 Zhang 2024 · #4 Wu 2021 · #5 Porcelli 2025 · #7 Alharthi 2023 ·
#9 Pham 2025 · #12 Chen 2025 · #13 Vogel 2024. (Foundational classics — Salfner 2010, Breiman 2001,
Kafka 2011 — are kept deliberately; a reliability-benchmark paper is expected to anchor on them.)

---

## 01 — AIOps & Failure-Management Surveys

Establish that fault injection is the standard way to generate failure ground truth, and that
sliding-window ML for imminent-failure prediction is an established AIOps pattern. These are the
"the field agrees this is a real problem" citations.

- **[1] Notaro, P., Cardoso, J., Gerndt, M. (2021).** *A Survey of AIOps Methods for Failure Management.*
  ACM Transactions on Intelligent Systems and Technology (TIST) 12(6), Article 81. DOI: 10.1145/3483424.
  → Canonical AIOps survey; frames fault-injection-generated ground truth + sliding-window prediction.
  *Metadata-only (paywalled ACM).*
- **[2] Zhang, L., Jia, T., Jia, M., Wu, Y., Liu, A., Yang, Y., Wu, Z., Hu, X., Yu, P.S., Li, Y. (2024).**
  *A Survey of AIOps for Failure Management in the Era of Large Language Models.* arXiv:2406.11213;
  ACM Computing Surveys, DOI: 10.1145/3746635.
  → The current (2024) recency anchor; supersedes leaning on Notaro alone. Author list independently
  re-confirmed (an earlier project draft had a fabricated "Chen, Z." placeholder — caught and fixed).
- **[3] Salfner, F., Lenk, M., Malek, M. (2010).** *A Survey of Online Failure Prediction Methods.*
  ACM Computing Surveys 42(3), Article 10, 1–42. DOI: 10.1145/1670679.1670680.
  → The foundational taxonomy of *online* (runtime, proactive) failure prediction — directly defines the
  "predict before it fails" framing and lead-time concept this project operationalizes. *Metadata-only.*

## 02 — ML Failure Prediction (adjacent domains)

Prior art that does ML-based failure prediction rigorously — but in **other** domains (hardware, HPC,
payments, cloud nodes, microservice RCA). Collectively they prove the method works and sharpen exactly
what's *unclaimed* for a fixed Kafka+Spark streaming pipeline.

- **[4] Wu, Z., Xu, H., Pang, G., Yu, F., Wang, Y., Jian, S., Wang, Y. (2021).** *DRAM Failure Prediction
  in AIOps: Empirical Evaluation, Challenges and Opportunities.* arXiv:2104.15052.
  → 7-classifier + 3-anomaly-detector empirical benchmark. *Hardware (DRAM); no lead-time metric.*
- **[5] Porcelli, L. (2025).** *A Feature Engineering Approach for Business Impact-Oriented Failure
  Detection in Distributed Instant Payment Systems.* arXiv:2510.21710. (single author, confirmed)
  → Recent proof the "domain-specific AIOps + explainable features" pattern is live. *Domain: payments.*
- **[6] Harrison, C., Kirkpatrick, C.R., Dutra, I. (2018).** *Bioinformatics Computational Cluster Batch
  Task Profiling with Machine Learning for Failure Prediction.* arXiv:1812.09537.
  → Random-Forest feature-selection job-failure prediction at production scale. *Batch HPC; no lead time.*
  (Author list corrected in this project — was wrongly recorded as "Ahmed, M., Fisher, D. et al.")
- **[7] Alharthi, K.A., Jhumka, A., Di, S., Gui, L., Cappello, F., McIntosh-Smith, S. (2023).**
  *Time Machine: Generative Real-Time Model for Failure (and Lead Time) Prediction in HPC Systems.*
  DSN 2023, pp. 508–521. DOI: 10.1109/DSN58367.2023.00054.
  → **Closest thematic neighbour in the whole collection** — the only prior work that predicts failure
  *and lead time* as explicit targets, like this project. *HPC log-event domain; no controlled fault
  injection; no static-threshold baseline; a deep-model contribution, not a benchmark.* Not a competitor.
- **[8] Lin, Q., Hsieh, K., Dang, Y., Zhang, H., Sui, K., Xu, Y., Lou, J.-G., Li, C., Wu, Y., Yao, R.,
  Chintalapati, M., Zhang, D. (2018).** *Predicting Node Failure in Cloud Service Systems.* ESEC/FSE 2018.
  DOI: 10.1145/3236024.3236060.
  → Microsoft production cloud node-failure prediction — the "real ops payoff of prediction" citation.
  *Node/VM granularity, not pipeline components; no lead-time-to-failure metric.*
- **[9] Pham, L., Zhang, H., Ha, H., Salim, F., Zhang, X. (2025).** *RCAEval: A Benchmark for Root Cause
  Analysis of Microservice Systems with Telemetry Data.* WWW'25 Companion, pp. 777–780. DOI: 10.1145/3701716.3715290 (arXiv:2412.17015).
  → Recent example of the *open telemetry benchmark + reproducible baselines* contribution pattern this
  project follows. *Root-cause analysis (post-hoc), not predictive lead time; microservices, not Kafka+Spark.*

## 03 — Fault Injection & Chaos Engineering

The methodological backbone: injecting real faults to study system behaviour. Justifies KSPFail's core
method and situates it against modern cloud-native chaos tooling.

- **[10] Basiri, A., Behnam, N., de Rooij, R., Hochstein, L., Kosewski, L., Reynolds, J., Rosenthal, C.
  (2016).** *Chaos Engineering.* IEEE Software 33(3), 35–41. DOI: 10.1109/MS.2016.60 (arXiv:1702.05843).
  → The canonical statement of controlled fault injection as an experimental discipline.
- **[11] Natella, R., Cotroneo, D., Madeira, H. (2016).** *Assessing Dependability with Software Fault
  Injection: A Survey.* ACM Computing Surveys 48(3), Article 44. DOI: 10.1145/2841425.
  → The rigorous survey of software fault injection — fault representativeness, efficiency, usability.
  Backstops the "are your injected faults representative?" reviewer question. *Metadata-only.*
- **[12] Chen, Z., Goudarzi, M., Nadjaran Toosi, A. (2025).** *Resilience Evaluation of Kubernetes in
  Cloud-Edge Environments via Failure Injection.* arXiv:2507.16109.
  → Very recent, methodologically parallel: injects node/pod/network faults into a *live* Kubernetes
  cluster and measures application-level effects (11,965 scenarios). Closest recent method-sibling to
  KSPFail's Kind-based injection. *Measures resilience/stability, not predictive ML on telemetry.*

## 04 — Stream Processing Reliability (Kafka / Spark)

The system-under-test's foundational and reliability literature. Includes the single closest *systems-
benchmark* neighbour (DEBS'24) and the platform papers themselves.

- **[13] Vogel, A., Henning, S., Perez-Wohlfeil, E., Ertl, O., Rabiser, R. (2024).** *A Comprehensive
  Benchmarking Analysis of Fault Recovery in Stream Processing Frameworks.* DEBS'24 (also arXiv:2404.06203).
  → **Closest adjacent systems benchmark**: chaos-style fault injection across Flink, Kafka Streams, Spark
  Structured Streaming on Kubernetes. *Measures post-hoc recovery speed across competing engines — not
  predictive ML, no lead-time metric, no labelled pre-failure dataset, no threshold-vs-ML comparison.*
- **[14] Al-Sayeh, H., Hagedorn, S., Sattler, K.-U. (2020).** *A gray-box modeling methodology for runtime
  prediction of Apache Spark jobs.* Distributed and Parallel Databases 38, 819–839. DOI: 10.1007/s10619-020-07286-y.
  → Establishes Spark *runtime/performance* prediction as a distinct, well-studied problem — a contrast
  that sharpens the framing (predicting execution time ≠ predicting failure).
- **[15] Kreps, J., Narkhede, N., Rao, J. (2011).** *Kafka: a Distributed Messaging System for Log
  Processing.* NetDB'11.
  → The Kafka system paper — cite when describing the pipeline under test.
- **[16] Armbrust, M., Das, T., Torres, J., Yavuz, B., Zhu, S., Xin, R., Ghodsi, A., Stoica, I.,
  Zaharia, M. (2018).** *Structured Streaming: A Declarative API for Real-Time Applications in Apache
  Spark.* SIGMOD 2018, pp. 601–613. DOI: 10.1145/3183713.3190664.
  → The Spark Structured Streaming system paper — the exact streaming engine benchmarked here.

## 05 — ML Methods & Explainability (methods actually used)

The models and interpretability method KSPFail runs. Cite these where the methodology names them; all
are stock, canonical implementations (no novel algorithm claimed — see `research_context.md` §7).

- **[17] Breiman, L. (2001).** *Random Forests.* Machine Learning 45(1), 5–32. DOI: 10.1023/A:1010933404324.
- **[18] Chen, T., Guestrin, C. (2016).** *XGBoost: A Scalable Tree Boosting System.* KDD'16, pp. 785–794.
  DOI: 10.1145/2939672.2939785 (arXiv:1603.02754).
- **[19] Ke, G., Meng, Q., Finley, T., Wang, T., Chen, W., Ma, W., Ye, Q., Liu, T.-Y. (2017).** *LightGBM:
  A Highly Efficient Gradient Boosting Decision Tree.* NeurIPS 2017 (NIPS 30), pp. 3146–3154.
- **[20] Lundberg, S.M., Lee, S.-I. (2017).** *A Unified Approach to Interpreting Model Predictions.*
  NeurIPS 2017 (NIPS 30). arXiv:1705.07874. → SHAP; the interpretability method used for mechanism checking.

---

## Considered but deliberately excluded

- **IJETCSIT (March 2026) Kafka+Spark healthcare benchmark** — surface-similar and recent, but the venue's
  indexing/standing could not be independently verified (not a recognised major venue). Kept as an
  *awareness* note in `research_context.md` §3 (a reviewer might find it), **not** admitted to this curated
  set. Do not cite until venue standing is confirmed.
- **Vendor/industry content** (Acceldata, Conduktor, IBM, AutoMQ blogs; a US XGBoost-lag patent) — real and
  useful for motivation, but not peer-reviewed. Referenced narratively in `research_context.md` §3, not here.

## Provenance flags

- **3 metadata-only** (#1 Notaro, #3 Salfner, #11 Natella): paywalled ACM surveys. DOIs are correct; free
  author copies exist on ResearchGate / Semantic Scholar; pull through institutional access before submission.
- **#7 Time Machine PDF** is the open-access author copy from the University of Warwick research repository
  (WRAP), not the paywalled IEEE Xplore version — content-identical, cite the DSN 2023 published record.

## Maintenance

This set mirrors and corrects `docs/research_context.md` §3 (Literature Review and Positioning). The novelty
gap it supports — *controlled fault injection + multi-model ML + lead-time evaluation + static-threshold
baseline on a fixed Kafka + Spark Structured Streaming pipeline* — was last re-confirmed open on 2026-07-16.
**Re-run a saturation search before final submission**; a novelty claim is only as current as its last search.
