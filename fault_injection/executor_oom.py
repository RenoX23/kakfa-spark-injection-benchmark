"""Fault class: executor OOM-kill (memory pressure induction). Section 6.2, locked taxonomy.

Unlike broker_kill.py (a clean pod delete), this needs to trigger a genuine kernel OOM
event inside the executor's cgroup, not just remove the pod -- that's the whole point of
this fault class as distinct from broker_kill. Induction: exec into the running executor
container and allocate memory well past its resource limit until the kernel OOM-killer fires.

Ground truth for the OOM event itself comes from the DRIVER's log, not the executor pod's
own status. First implementation checked the executor pod's lastState.terminated.reason
directly and it never matched: Spark's driver deletes a lost executor's pod object almost
immediately after detecting the loss, racing out any post-hoc inspection of that pod. The
driver's own log doesn't have that race -- it logs the loss durably ("Lost executor N ...
exited with exit code 137 (SIGKILL, possible container OOM)").
"""
import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))
from common import kubectl, now_iso, poll_until, write_ground_truth

FAULT_CLASS = "executor_oom_kill"
# The driver's "Lost executor N ..." and "...exit code 137..." land on separate log
# lines for the same event -- a plain ".*" regex doesn't span newlines by default in
# Python, so an earlier version of this pattern silently never matched. Checking for
# the exit-code line alone is sufficient and simpler.
OOM_EXIT_RE = re.compile(r"exit code 137.*possible container OOM")

# Gradual allocation ramp (2026-07-11 redesign, replacing the original single-shot
# bytearray(200MB)*20 injection). That original version was a true instant
# step-function -- confirmed: one unlooped list comprehension, no time.sleep anywhere
# in it, crossing the container's 1152Mi cgroup limit by roughly the 6th of 20 chunks,
# the whole thing done in well under a second. No available telemetry (Spark-level,
# cAdvisor-level, or kube-state-metrics-level, all checked against real fault windows)
# has ever caught it, because nothing scrapes faster than ~5-7s and the fault itself
# only lasted that long. Ramping toward the same 1152Mi limit over ~90-120s instead of
# hitting it in under a second, so a normal 60s Prometheus scrape interval has room to
# land mid-climb and see a genuine rising trend, not just a before/after snapshot pair.
RAMP_CHUNK_BYTES = 100 * 1024 * 1024  # 100MB per chunk
RAMP_CHUNKS = 16  # 1600MB if never killed -- safety margin past the 1152Mi limit
RAMP_SLEEP_S = 9  # crossing 1152Mi happens ~chunk 12 -> ~99s into the ramp (11 sleeps)


def find_pod(namespace, label):
    # check=False: kubectl errors (not just returns empty) when the jsonpath indexes into
    # an empty .items list -- an expected, non-exceptional case here (e.g. momentarily
    # between the old executor dying and the new one being scheduled), not a real failure.
    result = kubectl(
        "-n", namespace, "get", "pods", "-l", label,
        "-o", "jsonpath={.items[0].metadata.name}",
        check=False,
    )
    return result.stdout.strip() or None


def get_restart_count(namespace, pod):
    result = kubectl(
        "-n", namespace, "get", "pod", pod,
        "-o", "jsonpath={.status.containerStatuses[0].restartCount}",
        check=False,
    )
    return int(result.stdout.strip() or 0)


def driver_log_since(namespace, driver_pod, since_seconds):
    result = kubectl(
        "-n", namespace, "logs", driver_pod, f"--since={since_seconds}s",
        check=False,
    )
    return result.stdout


def run(run_id, namespace="spark", outdir="results/fault-runs"):
    os.makedirs(outdir, exist_ok=True)
    pod = find_pod(namespace, "spark-role=executor")
    driver_pod = find_pod(namespace, "spark-role=driver")
    if pod is None or driver_pod is None:
        raise RuntimeError("no running executor/driver pod found -- is the Spark job up?")
    baseline_restarts = get_restart_count(namespace, pod)

    inject_ts = now_iso()
    kubectl(
        "-n", namespace, "exec", pod, "--",
        "python3", "-c",
        "import time\n"
        "chunks = []\n"
        f"for i in range({RAMP_CHUNKS}):\n"
        f"    chunks.append(bytearray({RAMP_CHUNK_BYTES}))\n"
        f"    time.sleep({RAMP_SLEEP_S})\n",
        check=False,
    )

    oomkilled_ts = poll_until(
        lambda: OOM_EXIT_RE.search(driver_log_since(namespace, driver_pod, 90)) is not None,
        timeout=60,
        interval=3,
    )

    # A new executor pod gets scheduled by the driver after the old one is lost -- different
    # pod name (identity churn, same pattern as broker_kill's instance-IP churn finding), so
    # "recovery" means a NEW executor pod reaching Running, not the same pod name coming back.
    def new_executor_running():
        candidate = find_pod(namespace, "spark-role=executor")
        if candidate is None or candidate == pod:
            return False
        phase = kubectl(
            "-n", namespace, "get", "pod", candidate,
            "-o", "jsonpath={.status.phase}", check=False,
        ).stdout.strip()
        return phase == "Running"

    recovered_ts = poll_until(new_executor_running, timeout=120, interval=3)

    record = {
        "fault_class": FAULT_CLASS,
        "run_id": run_id,
        "namespace": namespace,
        "target_pod": pod,
        "driver_pod": driver_pod,
        "baseline_restart_count": baseline_restarts,
        "injection_timestamp_utc": inject_ts,
        "oomkilled_confirmed_utc": oomkilled_ts,
        "new_executor_recovered_utc": recovered_ts,
        "recovered": recovered_ts is not None,
    }
    path = write_ground_truth(record, os.path.join(outdir, f"{FAULT_CLASS}_run{run_id}.json"))
    print(f"wrote {path}: oomkilled={oomkilled_ts is not None}, recovered={record['recovered']}")
    return record


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--namespace", default="spark")
    args = parser.parse_args()
    run(args.run_id, namespace=args.namespace)
