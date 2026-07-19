# 04 — Stream Processing Reliability (Kafka / Spark)

The system-under-test's foundational and reliability literature. Includes the single closest *systems-
benchmark* neighbour (DEBS'24) and the platform papers themselves.

| File | Paper | Year | ID |
|------|-------|------|----|
| `vogel2024-debs-fault-recovery-benchmark.pdf` | Vogel et al. — *Benchmarking Fault Recovery in Stream Processing Frameworks* (DEBS'24) | 2024 | arXiv:2404.06203 |
| `alsayeh2020-graybox-spark-runtime.pdf` | Al-Sayeh, Hagedorn, Sattler — *Gray-box Runtime Prediction of Apache Spark Jobs* | 2020 | DOI 10.1007/s10619-020-07286-y |
| `kreps2011-kafka.pdf` | Kreps, Narkhede, Rao — *Kafka: a Distributed Messaging System for Log Processing* | 2011 | NetDB'11 |
| `armbrust2018-structured-streaming.pdf` | Armbrust et al. — *Structured Streaming (Apache Spark)* | 2018 | DOI 10.1145/3183713.3190664 |

**Vogel et al. / DEBS'24** is the closest adjacent systems benchmark — chaos-style fault injection across
Flink, Kafka Streams, Spark Structured Streaming — but measures post-hoc *recovery speed* across competing
engines, not predictive ML with lead time on a fixed pipeline. Kafka and Structured Streaming are the
platform papers for the pipeline under test. Full detail in [`../README.md`](../README.md).
