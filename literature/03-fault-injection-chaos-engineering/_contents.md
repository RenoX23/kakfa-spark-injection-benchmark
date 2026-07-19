# 03 — Fault Injection & Chaos Engineering

The methodological backbone: injecting real faults to study system behaviour. Justifies KSPFail's core
method and situates it against modern cloud-native chaos tooling.

| File | Paper | Year | ID |
|------|-------|------|----|
| `basiri2016-chaos-engineering.pdf` | Basiri et al. — *Chaos Engineering* | 2016 | DOI 10.1109/MS.2016.60 |
| *(metadata-only)* | Natella, Cotroneo, Madeira — *Assessing Dependability with Software Fault Injection: A Survey* | 2016 | DOI 10.1145/2841425 |
| `chen2025-k8s-failure-injection.pdf` | Chen, Goudarzi, Nadjaran Toosi — *Resilience Evaluation of Kubernetes via Failure Injection* | 2025 | arXiv:2507.16109 |

**Chen et al. (2025)** is the closest recent method-sibling: injects node/pod/network faults into a live
Kubernetes cluster (11,965 scenarios) — parallels KSPFail's Kind-based injection, but measures resilience,
not predictive ML. **Natella et al.** backstops the "are your injected faults representative?" reviewer
question (metadata-only; free author copy on ResearchGate/CISUC). Full detail in [`../README.md`](../README.md).
