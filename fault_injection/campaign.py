"""Weeks 4-5: the full fault-injection campaign. Runs each locked fault class N times
with randomized steady-state gaps between repetitions (Section 6.2: "at randomized
injection points during steady-state load, to avoid the model learning
injection-schedule artifacts instead of genuine pre-failure signal").

Reuses each fault class's own run() function directly rather than shelling out to the
individual scripts -- they're already plain importable Python functions.

Classes run sequentially (all reps of one class, then the next), not interleaved: simpler
to monitor and avoids compounding failure interactions between concurrent fault types on
a single-node cluster with everything sharing the same 5 CPUs.

A single failed repetition does not abort the campaign -- caught, logged into the
manifest with its error, and the campaign continues. Manifest is written incrementally
after every repetition, not just at the end, so an interrupted campaign (this
environment has already had two unrelated restarts this project) loses at most the
in-flight repetition, not the whole run's progress.

network_degradation uses harsher-than-script-default settings here deliberately: Weeks
2-3 found the script's mild defaults (300ms/10%/30s) are a coin-flip on whether
Prometheus's 60s scrape interval even catches the fault window. Campaign data needs a
reliable signal in every repetition, not roughly half of them.
"""
import argparse
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from common import kubectl, now_iso
import broker_kill
import executor_oom
import backpressure_cascade
import disk_pressure
import network_degradation


def preflight_check():
    """Verify live cluster state actually matches what the campaign assumes, before
    spending hours running it. Added after a real incident: the pilot run found the
    Kafka broker's live storage type had silently reverted to ephemeral (matching
    infra/kafka/kafka-single-broker.yaml's ORIGINAL Phase-0 state, not the
    persistent-claim fix committed since) sometime during an environment gap, even
    though the committed manifest was never wrong. Nothing caught this until a real
    broker_kill actually wiped data and cascaded into 4 downstream failures. This
    check exists so that class of drift fails fast with one clear error, not by
    silently burning through a chunk of a multi-hour campaign's repetitions."""
    problems = []

    storage_type = kubectl(
        "-n", "kafka", "get", "kafkanodepool", "single",
        "-o", "jsonpath={.spec.storage.volumes[0].type}", check=False,
    ).stdout.strip()
    if storage_type != "persistent-claim":
        problems.append(
            f"Kafka broker storage type is '{storage_type}', expected 'persistent-claim' -- "
            f"live state has drifted from infra/kafka/kafka-single-broker.yaml. "
            f"Fix: kubectl -n kafka delete kafka kspfail kafkanodepool single, "
            f"then kubectl apply -f infra/kafka/kafka-single-broker.yaml"
        )

    kafka_phase = kubectl(
        "-n", "kafka", "get", "pod", "kspfail-single-0",
        "-o", "jsonpath={.status.phase}", check=False,
    ).stdout.strip()
    if kafka_phase != "Running":
        problems.append(f"Kafka broker pod phase is '{kafka_phase}', expected 'Running'")

    for role in ("driver", "executor"):
        phase = kubectl(
            "-n", "spark", "get", "pods", "-l", f"spark-role={role}",
            "-o", "jsonpath={.items[0].status.phase}", check=False,
        ).stdout.strip()
        if phase != "Running":
            problems.append(f"Spark {role} pod phase is '{phase or 'MISSING'}', expected 'Running'")

    if problems:
        raise RuntimeError(
            "Pre-flight check failed, refusing to start campaign:\n"
            + "\n".join(f"  - {p}" for p in problems)
        )

FAULT_RUNNERS = {
    "broker_kill": lambda run_id, outdir: broker_kill.run(run_id, outdir=outdir),
    "executor_oom": lambda run_id, outdir: executor_oom.run(run_id, outdir=outdir),
    "backpressure_cascade": lambda run_id, outdir: backpressure_cascade.run(run_id, outdir=outdir),
    "disk_pressure": lambda run_id, outdir: disk_pressure.run(run_id, outdir=outdir),
    "network_degradation": lambda run_id, outdir: network_degradation.run(
        run_id, delay_ms=500, loss_pct=20, duration_s=90, outdir=outdir
    ),
}


def run_campaign(reps_per_class, classes=None, min_gap_s=45, max_gap_s=90,
                  outdir="results/campaign", manifest_path=None, skip_preflight=False):
    if not skip_preflight:
        preflight_check()
    classes = classes or list(FAULT_RUNNERS.keys())
    os.makedirs(outdir, exist_ok=True)
    manifest_path = manifest_path or os.path.join(outdir, "manifest.json")
    manifest = {"campaign_start_utc": now_iso(), "reps_per_class": reps_per_class, "runs": []}

    total = len(classes) * reps_per_class
    done = 0
    for cls in classes:
        runner = FAULT_RUNNERS[cls]
        class_outdir = os.path.join(outdir, cls)
        for i in range(1, reps_per_class + 1):
            run_id = f"campaign{i}"
            t0 = time.time()
            status, error = "ok", None
            try:
                runner(run_id, class_outdir)
            except Exception as e:
                status, error = "error", f"{type(e).__name__}: {e}"
            elapsed = time.time() - t0
            done += 1

            manifest["runs"].append({
                "fault_class": cls, "rep": i, "run_id": run_id,
                "status": status, "error": error, "elapsed_seconds": round(elapsed, 1),
                "started_utc": now_iso(),
            })
            with open(manifest_path, "w") as f:
                json.dump(manifest, f, indent=2)

            error_suffix = f" error={error}" if error else ""
            print(f"[{done}/{total}] {cls} rep {i}/{reps_per_class}: status={status} "
                  f"elapsed={elapsed:.1f}s{error_suffix}")

            is_last = (cls == classes[-1] and i == reps_per_class)
            if not is_last:
                gap = random.uniform(min_gap_s, max_gap_s)
                print(f"  cooldown {gap:.0f}s before next injection...")
                time.sleep(gap)

    manifest["campaign_end_utc"] = now_iso()
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    ok = sum(1 for r in manifest["runs"] if r["status"] == "ok")
    print(f"\ncampaign done: {ok}/{total} ok, manifest at {manifest_path}")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reps", type=int, default=15, help="repetitions per fault class")
    parser.add_argument("--classes", nargs="*", default=None,
                         help="subset of fault classes to run (default: all 5)")
    parser.add_argument("--min-gap-s", type=int, default=45)
    parser.add_argument("--max-gap-s", type=int, default=90)
    parser.add_argument("--outdir", default="results/campaign")
    parser.add_argument("--skip-preflight", action="store_true")
    args = parser.parse_args()
    run_campaign(args.reps, classes=args.classes, min_gap_s=args.min_gap_s,
                 max_gap_s=args.max_gap_s, outdir=args.outdir, skip_preflight=args.skip_preflight)
