"""Fault class: disk-pressure on broker. Section 6.2, locked taxonomy.

Originally planned as a Kubernetes ephemeral-storage resource limit (fill past a small,
explicit limit, let kubelet's real eviction mechanism fire). Turned out not to apply:
ephemeral-storage limits govern the container's writable layer and un-sized emptyDirs,
NOT PersistentVolumeClaim-backed volumes -- and our broker's data directory is a PVC
(deliberately, since Weeks 2-3's broker_kill fix). Switching the data volume back to
emptyDir to make that limit apply would undo that fix.

Instead: fill the broker's real (PVC-backed) data directory with a small, bounded amount
-- trivial against this machine's ~929GB free disk, so no risk to the shared host or its
other unrelated projects -- and observe the drop via node-exporter's
node_filesystem_avail_bytes (already scraped by Prometheus). This doesn't trigger a full
Kubernetes disk-pressure eviction, but it's arguably a better fit for what this
dissertation actually measures: gradual degradation telemetry (RO4's lead-time framing),
not just binary crash/recovery like broker_kill and executor_oom already cover.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from common import (
    kubectl,
    now_iso,
    poll_until,
    prom_query,
    start_port_forward,
    stop_port_forward,
    write_ground_truth,
)

FAULT_CLASS = "disk_pressure_broker"
DATA_DIR = "/var/lib/kafka/data-0"
FILL_FILE = f"{DATA_DIR}/kspfail_disk_pressure_fill.dat"


def avail_bytes(port, node="kspfail-control-plane"):
    # mountpoint="/" doesn't exist in this node-exporter's view -- Kind's node
    # container doesn't expose a plain "/" entry, the large disk shows up as "/var"
    # instead (verified directly: node_filesystem_avail_bytes{mountpoint="/"} returns
    # nothing, {mountpoint="/var"} returns ~996GB matching the host's real free space).
    result = prom_query(
        'node_filesystem_avail_bytes{mountpoint="/var", job="kubernetes-service-endpoints"}',
        port,
    )
    res = result["data"]["result"]
    if not res:
        return None
    # single-node cluster -- just take whichever series matches, there's only one node.
    return float(res[0]["value"][1])


def run(run_id, namespace="kafka", pod="kspfail-single-0", fill_gb=3, outdir="results/fault-runs"):
    os.makedirs(outdir, exist_ok=True)
    pf, port = start_port_forward("monitoring", "svc/prometheus-server", 80)
    try:
        baseline_avail = avail_bytes(port)

        inject_ts = now_iso()
        kubectl(
            "-n", namespace, "exec", pod, "--",
            "dd", "if=/dev/zero", f"of={FILL_FILE}", "bs=1M", f"count={fill_gb * 1024}",
            check=False,
        )
        fill_done_ts = now_iso()

        drop_confirmed_ts = poll_until(
            lambda: (a := avail_bytes(port)) is not None and baseline_avail is not None
            and (baseline_avail - a) > (fill_gb * 1024 * 1024 * 1024 * 0.8),
            timeout=60,
            interval=5,
        )
        post_fill_avail = avail_bytes(port)

        # Cleanup: remove the fill file and confirm space is reclaimed. timeout=150,
        # not 60: node-exporter's own scrape interval means a shorter window can
        # legitimately time out even though the underlying fs state already recovered
        # (confirmed directly with `df` when this first happened -- not a real bug).
        kubectl("-n", namespace, "exec", pod, "--", "rm", "-f", FILL_FILE, check=False)
        recovered_ts = poll_until(
            lambda: (a := avail_bytes(port)) is not None and baseline_avail is not None
            and (baseline_avail - a) < (fill_gb * 1024 * 1024 * 1024 * 0.2),
            timeout=150,
            interval=10,
        )

        record = {
            "fault_class": FAULT_CLASS,
            "run_id": run_id,
            "namespace": namespace,
            "target_pod": pod,
            "fill_size_gb": fill_gb,
            "baseline_avail_bytes": baseline_avail,
            "injection_timestamp_utc": inject_ts,
            "fill_done_utc": fill_done_ts,
            "post_fill_avail_bytes": post_fill_avail,
            "drop_confirmed_utc": drop_confirmed_ts,
            "cleaned_up_utc": recovered_ts,
            "recovered": recovered_ts is not None,
        }
        path = write_ground_truth(record, os.path.join(outdir, f"{FAULT_CLASS}_run{run_id}.json"))
        print(f"wrote {path}: drop_confirmed={drop_confirmed_ts is not None}, recovered={record['recovered']}")
        return record
    finally:
        stop_port_forward(pf)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--fill-gb", type=int, default=3)
    args = parser.parse_args()
    run(args.run_id, fill_gb=args.fill_gb)
