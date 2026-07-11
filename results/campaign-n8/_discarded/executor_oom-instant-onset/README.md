These 8 reps (executor_oom_kill_runcampaign1/5/6/7.json,
executor_oom_kill_runtopup1/2/3/4.json) used the original executor_oom injection design:
a single instant allocation of a bytearray sized to blow past the 1152Mi cgroup limit in
one shot. Ground-truth OOM detection (driver-log-based) worked fine on all of them --
that was never in question. What failed was detectability by any independent telemetry
signal: at Prometheus's 60s scrape interval, an instant step-function fault has zero
precursor duration by design, so container_memory_working_set_bytes and every other
scraped metric shows a single before/after jump with no buildup phase to sample multiple
points from. Confirmed empirically this session across several diagnostic passes
(kube-state-metrics restart-counter check -- negative, pod replacement not container
restart, structurally unobservable regardless of scrape interval; PySpark-level
executor_memoryUsed_bytes -- doesn't reflect the raw bytearray allocation; scrape-interval
reduction on spark-driver/cadvisor jobs -- ruled out, too risky / wrong layer).

Rather than keep searching for a faster or different telemetry signal to catch an
instant fault, the fault injection itself was redesigned: executor_oom.py now allocates
in 25MB chunks with a 7.5s sleep between chunks (RAMP_CHUNK_BYTES / RAMP_CHUNKS /
RAMP_SLEEP_S, calibrated against a measured baseline of ~484MB and a ~724MB gap to the
limit), targeting a 180-240s injection-to-OOM window. This restores a real, multi-point
rising signal in container_memory_working_set_bytes -- confirmed across the 8 reps that
replaced this set (executor_oom_kill_runramptest3.json through
executor_oom_kill_runramptest10.json in the parent executor_oom/ directory), all landing
in a 209-233s range with 3-4 genuine samples during the ramp, not interpolated or
single-step.

Kept as evidence that the instant-onset design was tried, worked correctly as a fault
injector, and was excluded specifically for undetectability -- not for being broken.
