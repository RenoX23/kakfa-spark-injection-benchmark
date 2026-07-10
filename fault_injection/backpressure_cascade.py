r"""Fault class: backpressure cascade (burst producer rate past consumer capacity).
Section 6.2, locked taxonomy.

Unlike broker_kill/executor_oom (kill something, watch it recover), this fault never
takes anything down -- the pipeline stays "up" the whole time, it just falls behind.
Ground truth reuses the existing `ts` field already in every message (no new
instrumentation): burst-produce far more messages than Spark's 5s trigger interval can
absorb, then measure the gap between wall-clock now() and the `ts` of the most recent
*steady-stream* record Spark has printed. While backlogged, that gap is large (Spark is
still working through old messages); it shrinks back to near-zero once Spark catches up.

Note: TS_ROW_RE matches `\d+` for seq, which doesn't match the burst's negative-seq
marker rows (`-$i`) -- lag is read off the regular positive-seq producer's stream, not
the burst records themselves. Still a valid lag proxy (the steady stream is equally
backlogged behind the burst), just worth being precise about which stream is measured.
"""
import argparse
import datetime
import os
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))
from common import kubectl, now_iso, poll_until, write_ground_truth

FAULT_CLASS = "backpressure_cascade"
TS_ROW_RE = re.compile(r"\|(\d+)\|(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)\|")
CAUGHT_UP_THRESHOLD_SECONDS = 10


def find_driver_pod(namespace):
    result = kubectl(
        "-n", namespace, "get", "pods", "-l", "spark-role=driver",
        "-o", "jsonpath={.items[0].metadata.name}", check=False,
    )
    name = result.stdout.strip()
    if not name:
        raise RuntimeError("no running driver pod found -- is the Spark job up?")
    return name


def latest_processed_ts(spark_namespace, driver_pod, tail_lines=200):
    """Returns (seq, ts) of the last data row seen in recent driver log output, or None."""
    result = kubectl(
        "-n", spark_namespace, "logs", driver_pod, f"--tail={tail_lines}", check=False,
    )
    matches = TS_ROW_RE.findall(result.stdout)
    if not matches:
        return None
    seq, ts = matches[-1]
    return int(seq), datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=datetime.timezone.utc
    )


def lag_seconds(spark_namespace, driver_pod):
    latest = latest_processed_ts(spark_namespace, driver_pod)
    if latest is None:
        return None
    _, ts = latest
    return (datetime.datetime.now(datetime.timezone.utc) - ts).total_seconds()


def burst_produce(kafka_namespace, broker_pod, topic, count):
    # One-shot burst via the same kafka-console-producer.sh the steady load generator
    # uses, but with no sleep between lines -- fires `count` messages as fast as the
    # shell loop and the broker can accept them, deliberately overwhelming the 5s
    # micro-batch trigger interval on the Spark side.
    script = (
        f"i=0; while [ $i -lt {count} ]; do "
        f'i=$((i+1)); echo "{{\\"seq\\":-$i,\\"ts\\":\\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\\",\\"value\\":$((RANDOM % 1000)),\\"burst\\":true}}"; '
        f"done | bin/kafka-console-producer.sh --topic {topic} --bootstrap-server localhost:9092 "
        f"--producer-property enable.idempotence=false"
    )
    kubectl("-n", kafka_namespace, "exec", broker_pod, "--", "bash", "-c", script, check=False)


def run(run_id, burst_size=3000, kafka_namespace="kafka", broker_pod="kspfail-single-0",
        spark_namespace="spark", topic="pipeline-events", outdir="results/fault-runs"):
    os.makedirs(outdir, exist_ok=True)
    driver_pod = find_driver_pod(spark_namespace)

    baseline_lag = lag_seconds(spark_namespace, driver_pod)
    if baseline_lag is None:
        raise RuntimeError("refusing to inject: no processed records found in driver log yet")
    if baseline_lag > CAUGHT_UP_THRESHOLD_SECONDS:
        raise RuntimeError(
            f"refusing to inject: baseline lag is {baseline_lag}s, not caught up before injection"
        )

    inject_ts = now_iso()
    burst_produce(kafka_namespace, broker_pod, topic, burst_size)
    burst_sent_ts = now_iso()

    peak_lag = 0.0
    deadline_hits = {"peak": peak_lag}

    def caught_up():
        lag = lag_seconds(spark_namespace, driver_pod)
        if lag is not None and lag > deadline_hits["peak"]:
            deadline_hits["peak"] = lag
        return lag is not None and lag <= CAUGHT_UP_THRESHOLD_SECONDS

    # Give the burst a moment to actually land and lag to build before polling for recovery.
    import time
    time.sleep(5)
    recovered_ts = poll_until(caught_up, timeout=180, interval=3)

    record = {
        "fault_class": FAULT_CLASS,
        "run_id": run_id,
        "burst_size": burst_size,
        "baseline_lag_seconds": baseline_lag,
        "injection_timestamp_utc": inject_ts,
        "burst_sent_timestamp_utc": burst_sent_ts,
        "peak_lag_seconds_observed": deadline_hits["peak"],
        "caught_up_utc": recovered_ts,
        "recovered": recovered_ts is not None,
    }
    path = write_ground_truth(record, os.path.join(outdir, f"{FAULT_CLASS}_run{run_id}.json"))
    print(f"wrote {path}: peak_lag={deadline_hits['peak']:.1f}s, recovered={record['recovered']}")
    return record


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--burst-size", type=int, default=3000)
    args = parser.parse_args()
    run(args.run_id, burst_size=args.burst_size)
