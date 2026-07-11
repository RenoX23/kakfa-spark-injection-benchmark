backpressure_cascade_runtopup1.json: not discarded for a data-quality problem in the rep
itself (peak_lag_seconds_observed=16.34s, recovered=true, both look fine) -- discarded
for unresolved config provenance.

The original rep1 (results/campaign-n8/manifest.json, campaign1, injected
2026-07-11T10:49:32Z) failed outright with "no running driver pod found" -- a genuine
driver-level absence, not an executor hiccup, and backpressure_cascade's own injection
logic never touches executor/driver state (burst-produce only). This happened in the
minute immediately after the executor_oom class finished, which by then had already
racked up 5 real executor kills against the true default maxNumFailures ceiling of 3
(silently in effect the whole time -- the config key set to raise it,
spark.kubernetes.executor.maxNumFailures, was not a real Spark key; see
infra/spark/submit-pod.yaml's "CORRECTED 2026-07-11" comment and
_discarded/executor_oom-old-maxfailures/README.md). A driver exhausting its
executor-failure budget and going down right as the next class's first rep starts is a
coherent explanation, but not provable after the fact: no driver log was being captured
for that time window (capture didn't start until 2026-07-11T13:57:44Z, hours later), so
there's no surviving evidence to confirm or rule out the mechanism.

This topup1 replacement (run 2026-07-11T15:00:20Z) doesn't resolve the question --
it was collected after the wrong-key "fix" (2026-07-11T14:53:09Z) but before the real
fix (2026-07-11T16:47:40Z), meaning it was ALSO collected while maxNumFailures was
silently still at its true default. Discarded on provenance grounds alone, not because
anything is wrong with the data point itself. Replaced by
backpressure_cascade_runrefresh1.json (injected 2026-07-11T18:15:49Z, well after the
real fix, peak_lag=9.6s, recovered=true) in the parent backpressure_cascade/ directory.
