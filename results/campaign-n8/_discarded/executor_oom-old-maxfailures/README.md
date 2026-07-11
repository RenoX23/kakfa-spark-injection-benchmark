executor_oom_kill_runcampaign8.json: `recovered: false`. Its `target_pod` was
`...exec-4` -- reps 5/6/7 had already consumed replacements 1/2/3 for that driver
(default `spark.kubernetes.executor.maxNumFailures = max(numExecutors*2,3) = 3` for
`spark.executor.instances=1`), so rep8's kill was the 4th failure and Spark refused to
schedule a replacement. Same root cause as the `[INTERNAL_ERROR]` crashes fixed in
`infra/spark/submit-pod.yaml` (maxNumFailures raised to 50). Its `detection_latency_s`
(injection -> OOM confirmed, 1.0s) was itself clean and not the reason for discarding --
`recovered: false` is. Kept as evidence, excluded from the active dataset. Replaced by
`executor_oom_kill_runtopup4.json` in the parent `executor_oom/` directory, run after
the maxNumFailures fix was live.

executor_oom_kill_runramptest2.json: same root cause, different symptom. `recovered:
false`, `new_executor_recovered_utc: null`. This was the calibration rep that confirmed
the recalibrated gradual-ramp design (226s, `injection_timestamp_utc` 2026-07-11T16:31:07Z
-> `oomkilled_confirmed_utc` 2026-07-11T16:34:53Z) -- that part of the rep is genuine and
is what the ramp redesign was validated against. But its OOM landed on the driver's 4th
executor failure while the wrong config key (`spark.kubernetes.executor.maxNumFailures`,
silently ignored -- see `infra/spark/submit-pod.yaml`'s "CORRECTED 2026-07-11" comment)
was still live, so the driver crashed immediately after OOM confirmation, before a
replacement executor could be observed. Duration/detection is trustworthy; lifecycle
completeness is not. Excluded from the final 8-rep ramp-design dataset. Replaced by
`executor_oom_kill_runramptest10.json` (run after the fix, 223s, `recovered: true`,
0 crashes across this and the 7 reps run immediately before it) in the parent
`executor_oom/` directory.
