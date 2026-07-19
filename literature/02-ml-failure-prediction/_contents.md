# 02 — ML Failure Prediction (adjacent domains)

ML-based failure prediction done rigorously — but in other domains. Proves the method works; sharpens
what stays unclaimed for a fixed Kafka+Spark streaming pipeline.

| File | Paper | Year | ID |
|------|-------|------|----|
| `wu2021-dram-failure-prediction.pdf` | Wu et al. — *DRAM Failure Prediction in AIOps* | 2021 | arXiv:2104.15052 |
| `porcelli2025-payment-failure-detection.pdf` | Porcelli — *Failure Detection in Distributed Instant Payment Systems* | 2025 | arXiv:2510.21710 |
| `harrison2018-hpc-batch-failure-prediction.pdf` | Harrison, Kirkpatrick, Dutra — *Batch Task Profiling with ML for Failure Prediction* | 2018 | arXiv:1812.09537 |
| `alharthi2023-time-machine.pdf` | Alharthi et al. — *Time Machine: Failure (and Lead Time) Prediction in HPC* | 2023 | DOI 10.1109/DSN58367.2023.00054 |
| `lin2018-node-failure-prediction.pdf` | Lin et al. — *Predicting Node Failure in Cloud Service Systems* | 2018 | DOI 10.1145/3236024.3236060 |
| `pham2024-rcaeval-benchmark.pdf` | Pham et al. — *RCAEval: RCA Benchmark with Telemetry Data* | 2025 | WWW'25 Companion · arXiv:2412.17015 |

**Time Machine (2023)** is the closest thematic neighbour in the whole collection — it too predicts failure
*and lead time* — but different domain, no controlled fault injection, no threshold baseline; not a competitor.
Time Machine PDF is the open-access Warwick (WRAP) author copy; cite the DSN 2023 record. Full detail in
[`../README.md`](../README.md).
