"""Fault class: broker kill / restart (pod delete). Section 6.2, locked taxonomy.

Reusable, scriptable version of the Phase 0 manual pilot (results/phase0-pilot-fault/).
Run repeatedly (N>=15-20, Weeks 4-5 campaign) via: python broker_kill.py --run-id N
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from common import (
    kubectl,
    now_iso,
    poll_until,
    prom_target_health,
    start_port_forward,
    stop_port_forward,
    write_ground_truth,
)

FAULT_CLASS = "broker_kill_pod_delete"


def run(run_id, namespace="kafka", pod="kspfail-single-0", scrape_pool="kafka-broker-jmx", outdir="results/fault-runs"):
    os.makedirs(outdir, exist_ok=True)
    pf, port = start_port_forward("monitoring", "svc/prometheus-server", 80)
    try:
        baseline = prom_target_health(scrape_pool, port)
        if baseline != "up":
            raise RuntimeError(f"refusing to inject: target health is '{baseline}', not 'up' before injection")

        inject_ts = now_iso()
        kubectl("-n", namespace, "delete", "pod", pod, "--wait=false")

        down_ts = poll_until(lambda: prom_target_health(scrape_pool, port) != "up", timeout=60, interval=2)
        recovered_ts = poll_until(lambda: prom_target_health(scrape_pool, port) == "up", timeout=120, interval=3)

        record = {
            "fault_class": FAULT_CLASS,
            "run_id": run_id,
            "namespace": namespace,
            "target_pod": pod,
            "injection_timestamp_utc": inject_ts,
            "target_unhealthy_detected_utc": down_ts,
            "target_recovered_utc": recovered_ts,
            "recovered": recovered_ts is not None,
        }
        path = write_ground_truth(record, os.path.join(outdir, f"{FAULT_CLASS}_run{run_id}.json"))
        print(f"wrote {path}: recovered={record['recovered']}")
        return record
    finally:
        stop_port_forward(pf)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--namespace", default="kafka")
    parser.add_argument("--pod", default="kspfail-single-0")
    args = parser.parse_args()
    run(args.run_id, namespace=args.namespace, pod=args.pod)
