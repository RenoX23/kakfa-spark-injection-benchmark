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
