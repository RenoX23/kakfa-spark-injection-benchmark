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
import datetime
import json
import os
import random
import statistics
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from common import (
    kubectl,
    now_iso,
    prom_query_range,
    start_port_forward,
    stop_port_forward,
)
import broker_kill
import executor_oom
import backpressure_cascade
import disk_pressure
import network_degradation

# Metric checked per fault class for the mid-run integrity checkpoint -- chosen to be
# the metric most directly tied to what that class actually touches, not a generic
# "is Prometheus alive" check.
CLASS_METRIC_CHECK = {
    "broker_kill": 'up{job="kafka-broker-jmx"}',
    "executor_oom": 'up{job="spark-driver"}',
    "backpressure_cascade": 'up{job="spark-driver"}',
    "disk_pressure": 'node_filesystem_avail_bytes{mountpoint="/var"}',
    "network_degradation": 'scrape_duration_seconds{job="kafka-broker-jmx"}',
}

CHECKPOINT_LOG_DEFAULT = ".claude/campaign_checkpoint.log"

# Signature of a Spark-internal crash observed twice during the N=8 run (2026-07-11),
# both times during executor_oom activity: "[INTERNAL_ERROR] The Spark SQL phase
# optimization failed with an internal error". Root cause NOT identified -- both
# crashed driver pods were deleted during manual recovery before their logs could be
# captured, and by the time this wrapper was written the K8s events for that window had
# already expired (default 1h TTL). Rather than block the campaign on an undiagnosed
# Spark bug, this treats crash+recovery as an operational metric: detect, capture
# whatever log evidence is still available, auto-restart, log it, keep going.
SPARK_CRASH_SIGNATURE = "[INTERNAL_ERROR]"
SUBMIT_POD_YAML = os.path.join(os.path.dirname(__file__), "..", "infra", "spark", "submit-pod.yaml")
SPARK_DEPENDENT_CLASSES = {"executor_oom", "backpressure_cascade"}

# The 5 fault classes don't share a common ground-truth shape, so there's no single
# uniform "lead time" field to compare across all of them honestly. Each entry says what
# is actually being measured and whether it's a latency (time from injection to a
# confirmed signal) or a magnitude (how big the observed effect was) -- mixing the two
# kinds without labeling them would make a "class X has unusually wide variance"
# comparison misleading.
TIMING_METRIC = {
    "broker_kill": ("detection_latency_s", "latency", "injection -> target unhealthy detected"),
    "executor_oom": ("detection_latency_s", "latency", "injection -> OOM confirmed"),
    "disk_pressure": ("detection_latency_s", "latency", "injection -> disk-usage drop confirmed"),
    "backpressure_cascade": ("peak_lag_s", "magnitude", "peak observed processing lag"),
    "network_degradation": ("scrape_duration_delta_s", "magnitude", "peak-minus-baseline scrape duration"),
}


def _iso_to_dt(ts):
    return datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)


def _extract_timing_value(cls, record):
    """Pull this class's one timing/magnitude measurement out of a ground-truth record.
    Returns None if the relevant fields aren't present (e.g. detection never confirmed
    within timeout) -- excluded from variance stats rather than treated as zero."""
    try:
        if cls == "broker_kill":
            if record.get("target_unhealthy_detected_utc") and record.get("injection_timestamp_utc"):
                return (_iso_to_dt(record["target_unhealthy_detected_utc"])
                        - _iso_to_dt(record["injection_timestamp_utc"])).total_seconds()
        elif cls == "executor_oom":
            if record.get("oomkilled_confirmed_utc") and record.get("injection_timestamp_utc"):
                return (_iso_to_dt(record["oomkilled_confirmed_utc"])
                        - _iso_to_dt(record["injection_timestamp_utc"])).total_seconds()
        elif cls == "disk_pressure":
            if record.get("drop_confirmed_utc") and record.get("injection_timestamp_utc"):
                return (_iso_to_dt(record["drop_confirmed_utc"])
                        - _iso_to_dt(record["injection_timestamp_utc"])).total_seconds()
        elif cls == "backpressure_cascade":
            return record.get("peak_lag_seconds_observed")
        elif cls == "network_degradation":
            base = record.get("baseline_scrape_duration_seconds")
            peak = record.get("peak_scrape_duration_seconds_during_fault")
            if base is not None and peak is not None:
                return peak - base
    except (ValueError, KeyError):
        return None
    return None


def compute_timing_stats(cls, class_outdir):
    metric_name, metric_kind, metric_desc = TIMING_METRIC[cls]
    values = []
    if os.path.isdir(class_outdir):
        for fname in sorted(os.listdir(class_outdir)):
            if not fname.endswith(".json"):
                continue
            with open(os.path.join(class_outdir, fname)) as f:
                record = json.load(f)
            v = _extract_timing_value(cls, record)
            if v is not None:
                values.append(v)

    stats = {
        "metric_name": metric_name, "metric_kind": metric_kind, "metric_desc": metric_desc,
        "n": len(values), "values": [round(v, 2) for v in values],
    }
    if values:
        stats["min"] = round(min(values), 2)
        stats["max"] = round(max(values), 2)
        stats["mean"] = round(statistics.mean(values), 2)
        stats["stdev"] = round(statistics.stdev(values), 2) if len(values) >= 2 else 0.0
    return stats


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


def spark_pods_healthy(namespace="spark"):
    for role in ("driver", "executor"):
        phase = kubectl(
            "-n", namespace, "get", "pods", "-l", f"spark-role={role}",
            "-o", "jsonpath={.items[0].status.phase}", check=False,
        ).stdout.strip()
        if phase != "Running":
            return False
    return True


def capture_crash_evidence(namespace="spark"):
    """Best-effort grab of whatever the crashed driver pod's log still has, before
    relaunch deletes it. The pod is typically still present in Error/Failed phase at
    this point (Spark-on-k8s doesn't self-delete on crash) -- this is the only window
    to see it. Returns (signature_matched: bool, snippet: str)."""
    pod_name = kubectl(
        "-n", namespace, "get", "pods", "-l", "spark-role=driver",
        "-o", "jsonpath={.items[0].metadata.name}", check=False,
    ).stdout.strip()
    if not pod_name:
        return False, "no driver pod present to inspect (already deleted)"
    log = kubectl("-n", namespace, "logs", pod_name, "--tail=300", check=False).stdout
    if SPARK_CRASH_SIGNATURE in log:
        idx = log.find(SPARK_CRASH_SIGNATURE)
        return True, log[max(0, idx - 200):idx + 400]
    return False, (log[-400:] if log else "empty log")


def heal_spark(namespace="spark", timeout=180):
    """Delete the driver/executor/submit-runner pods and reapply submit-pod.yaml --
    the same manual recovery used live during the N=8 run's two crashes, now automated.
    Raises if Spark doesn't come back healthy within timeout (a heal that doesn't work
    is a fundamental blocker, not something to silently retry forever)."""
    kubectl("-n", namespace, "delete", "pod", "-l", "spark-role=driver",
            "--ignore-not-found", check=False)
    kubectl("-n", namespace, "delete", "pod", "-l", "spark-role=executor",
            "--ignore-not-found", check=False)
    kubectl("-n", namespace, "delete", "pod", "spark-submit-runner",
            "--ignore-not-found", check=False)
    kubectl("apply", "-f", SUBMIT_POD_YAML, check=False)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if spark_pods_healthy(namespace):
            return
        time.sleep(5)
    raise RuntimeError(f"heal_spark: Spark not healthy {timeout}s after relaunch -- giving up")


def _current_driver_pod(namespace="spark"):
    return kubectl(
        "-n", namespace, "get", "pods", "-l", "spark-role=driver",
        "-o", "jsonpath={.items[0].metadata.name}", check=False,
    ).stdout.strip() or None


def start_driver_log_capture(driver_log_dir, namespace="spark"):
    """Pipe `kubectl logs -f <driver-pod>` to a file on local disk for the life of the
    pod. This is what was missing during the N=8 run: the crashed driver pod's log
    lived only inside the pod and was gone the moment it was deleted during recovery,
    and by the time anyone went looking, kubelet had also GC'd the node-level log file
    and the K8s events had expired. Streaming to local disk as it happens means the
    trace survives pod deletion regardless of what cleans up the pod afterwards.
    Returns None if there's no driver pod up yet to attach to (caller should retry via
    ensure_driver_log_capture once one exists)."""
    pod_name = _current_driver_pod(namespace)
    if not pod_name:
        return None
    os.makedirs(driver_log_dir, exist_ok=True)
    path = os.path.join(driver_log_dir, f"{pod_name}_{now_iso().replace(':', '')}.log")
    f = open(path, "w")
    proc = subprocess.Popen(
        ["kubectl", "-n", namespace, "logs", "-f", pod_name],
        stdout=f, stderr=subprocess.STDOUT,
    )
    print(f"  [driver-log-capture] streaming {pod_name} -> {path}")
    return {"proc": proc, "file": f, "pod_name": pod_name, "path": path}


def stop_driver_log_capture(capture):
    if not capture:
        return
    capture["proc"].terminate()
    try:
        capture["proc"].wait(timeout=5)
    except subprocess.TimeoutExpired:
        capture["proc"].kill()
    capture["file"].close()


def ensure_driver_log_capture(capture, driver_log_dir, namespace="spark"):
    """(Re)start capture if there's no active capture, the captured pod no longer
    matches the live driver pod (heal_spark recreated it), or the capture process has
    exited on its own (e.g. the pod died out from under `kubectl logs -f`). Call this
    at the start of every spark-dependent class's rep loop and right after every
    heal_spark() -- both are points where the live driver pod can have changed."""
    live_pod = _current_driver_pod(namespace)
    if capture and capture["pod_name"] == live_pod and capture["proc"].poll() is None:
        return capture
    if capture:
        stop_driver_log_capture(capture)
    return start_driver_log_capture(driver_log_dir, namespace)


def handle_spark_crash(cls, rep, crash_events, checkpoint_path=CHECKPOINT_LOG_DEFAULT):
    """Called when Spark is found unhealthy (either proactively before a rep, or
    reactively after a rep raised). Captures evidence, heals, logs a block into the
    checkpoint log's crash-recovery section, and appends to the in-memory crash_events
    list that ends up in the manifest."""
    matched, snippet = capture_crash_evidence()
    heal_spark()
    event = {
        "detected_utc": now_iso(), "fault_class_active": cls, "rep_lost": rep,
        "signature_matched": matched, "log_snippet": snippet,
    }
    crash_events.append(event)
    block = (
        f"--- SPARK CRASH RECOVERY --- {event['detected_utc']}\n"
        f"fault_class_active: {cls}\n"
        f"rep_lost: {rep}\n"
        f"signature_matched ('{SPARK_CRASH_SIGNATURE}'): {matched}\n"
        f"log_snippet: {snippet!r}\n"
        f"action: driver+executor+submit-runner deleted, submit-pod.yaml reapplied, "
        f"healthy again as of {now_iso()}\n"
    )
    os.makedirs(os.path.dirname(checkpoint_path) or ".", exist_ok=True)
    with open(checkpoint_path, "a") as f:
        f.write(block + "\n")
    print(block)


def integrity_checkpoint(cls, class_outdir, expected_reps, class_start_iso,
                          checkpoint_path=CHECKPOINT_LOG_DEFAULT):
    """Run after each fault class's repetitions complete, not just once at the very end
    of the whole campaign -- if something breaks mid-run, this pinpoints exactly which
    class and which check failed, rather than a single pass/fail verdict discovered
    hours later. Three checks, independent of the per-run ok/error status already in
    the manifest (a rep can report "ok" and still fail one of these):

    1. record_count -- how many ground-truth JSON files this class's reps actually
       wrote to class_outdir. Distinct from "how many reps were attempted": a rep can
       run without raising and still fail to persist evidence.
    2. kafka_storage_mode -- re-checks the exact live-state-drift bug found during the
       first campaign pilot (this class's own storage silently reverting from
       persistent-claim to ephemeral, invisible until a broker_kill exercised it).
       Checked after every class, not just once at campaign start, because the pilot's
       drift wasn't present at any single checkpoint -- it happened silently sometime
       during a multi-hour gap. A one-time pre-flight check can't catch a regression
       that occurs mid-campaign.
    3. prometheus_data_nonempty -- a *range* query over this class's own actual run
       window (its first rep's start time to now), not an instant snapshot -- confirms
       Prometheus genuinely captured telemetry during this specific class's activity,
       not just that the scrape endpoint is alive in general.

    Also computes and logs timing/magnitude variance stats (min/max/mean/stdev) across
    this class's own reps -- see TIMING_METRIC for what's actually measured per class
    (not a uniform "lead time," the 5 classes don't share a common ground-truth shape).
    This is diagnostic, not a pass/fail gate: wide variance at N=8 doesn't mean
    something's broken, it's exactly the signal needed to decide whether a class needs
    more repetitions before the dataset is usable.
    """
    record_count = len(
        [f for f in os.listdir(class_outdir) if f.endswith(".json")]
    ) if os.path.isdir(class_outdir) else 0

    storage_type = kubectl(
        "-n", "kafka", "get", "kafkanodepool", "single",
        "-o", "jsonpath={.spec.storage.volumes[0].type}", check=False,
    ).stdout.strip()
    storage_ok = storage_type == "persistent-claim"

    prom_ok, prom_detail = False, "check did not run"
    try:
        pf, port = start_port_forward("monitoring", "svc/prometheus-server", 80)
        try:
            end_iso = now_iso()
            result = prom_query_range(
                CLASS_METRIC_CHECK[cls], class_start_iso, end_iso, "15s", port
            )
            series = result.get("data", {}).get("result", [])
            total_points = sum(len(s.get("values", [])) for s in series)
            prom_ok = total_points > 0
            prom_detail = (
                f"{len(series)} series, {total_points} data points for "
                f"'{CLASS_METRIC_CHECK[cls]}' over {class_start_iso}..{end_iso}"
            )
        finally:
            stop_port_forward(pf)
    except Exception as e:
        prom_detail = f"{type(e).__name__}: {e}"

    status = "PASS" if (record_count >= 1 and storage_ok and prom_ok) else "FAIL"

    timing = compute_timing_stats(cls, class_outdir)
    if timing["n"] >= 2:
        timing_line = (
            f"timing ({timing['metric_kind']} -- {timing['metric_desc']}): "
            f"n={timing['n']} min={timing['min']}s max={timing['max']}s "
            f"mean={timing['mean']}s stdev={timing['stdev']}s values={timing['values']}"
        )
    elif timing["n"] == 1:
        timing_line = (
            f"timing ({timing['metric_kind']} -- {timing['metric_desc']}): "
            f"n=1, single value={timing['values'][0]}s -- need n>=2 for stdev"
        )
    else:
        timing_line = (
            f"timing ({timing['metric_kind']} -- {timing['metric_desc']}): "
            f"n=0 -- no repetitions produced a usable {timing['metric_name']} value"
        )

    block = (
        f"=== CHECKPOINT: {cls} === {now_iso()}\n"
        f"record_count: {record_count} (expected {expected_reps})\n"
        f"kafka_storage_mode: {storage_type or 'UNKNOWN'} "
        f"({'OK' if storage_ok else 'REVERTED -- expected persistent-claim'})\n"
        f"prometheus_data_nonempty: {prom_ok} ({prom_detail})\n"
        f"{timing_line}\n"
        f"STATUS: {status}\n"
    )
    os.makedirs(os.path.dirname(checkpoint_path) or ".", exist_ok=True)
    with open(checkpoint_path, "a") as f:
        f.write(block + "\n")
    print(block)

    return {
        "fault_class": cls, "record_count": record_count, "expected_reps": expected_reps,
        "storage_type": storage_type, "storage_ok": storage_ok,
        "prom_ok": prom_ok, "prom_detail": prom_detail, "status": status,
        "timing": timing,
    }


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
                  outdir="results/campaign", manifest_path=None, skip_preflight=False,
                  checkpoint_path=CHECKPOINT_LOG_DEFAULT, driver_log_dir=None):
    if not skip_preflight:
        preflight_check()
    classes = classes or list(FAULT_RUNNERS.keys())
    os.makedirs(outdir, exist_ok=True)
    driver_log_dir = driver_log_dir or os.path.join(outdir, "driver_logs")
    manifest_path = manifest_path or os.path.join(outdir, "manifest.json")
    manifest = {"campaign_start_utc": now_iso(), "reps_per_class": reps_per_class, "runs": [],
                "checkpoints": [], "spark_crashes": []}
    crash_events = manifest["spark_crashes"]
    capture = None

    # Fresh log per campaign invocation, not appended across unrelated runs -- a stale
    # checkpoint block from a previous campaign sitting above this run's would be
    # confusing to read, especially mid-run while actively watching it.
    os.makedirs(os.path.dirname(checkpoint_path) or ".", exist_ok=True)
    with open(checkpoint_path, "w") as f:
        f.write(f"campaign checkpoint log -- started {now_iso()}, reps_per_class={reps_per_class}\n\n")

    total = len(classes) * reps_per_class
    done = 0
    for cls in classes:
        runner = FAULT_RUNNERS[cls]
        class_outdir = os.path.join(outdir, cls)
        class_start_iso = now_iso()

        if cls in SPARK_DEPENDENT_CLASSES:
            capture = ensure_driver_log_capture(capture, driver_log_dir)

        for i in range(1, reps_per_class + 1):
            run_id = f"campaign{i}"

            # Proactive: catch damage left over from a crash that already happened
            # (e.g. mid previous rep, or during the cooldown gap) before wasting an
            # attempt on a dead cluster -- this is what would have saved reps 3 and 4
            # of executor_oom in the N=8 run, which both failed the same way as rep 2
            # because nothing healed Spark between them.
            if cls in SPARK_DEPENDENT_CLASSES and not spark_pods_healthy():
                print(f"  [pre-rep check] Spark unhealthy before {cls} rep {i} -- healing...")
                handle_spark_crash(cls, i, crash_events, checkpoint_path=checkpoint_path)
                capture = ensure_driver_log_capture(capture, driver_log_dir)

            t0 = time.time()
            status, error = "ok", None
            try:
                runner(run_id, class_outdir)
            except Exception as e:
                status, error = "error", f"{type(e).__name__}: {e}"
                # Reactive: this rep's own failure may have BEEN the crash. Only treat
                # it as a Spark-crash event (and heal) if Spark is actually unhealthy --
                # a class-specific validation error (e.g. "baseline lag too high") isn't
                # a crash and healing for it would be pointless churn.
                if cls in SPARK_DEPENDENT_CLASSES and not spark_pods_healthy():
                    print(f"  [post-rep check] {cls} rep {i} failed and Spark is unhealthy -- healing...")
                    handle_spark_crash(cls, i, crash_events, checkpoint_path=checkpoint_path)
                    capture = ensure_driver_log_capture(capture, driver_log_dir)
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

        checkpoint_result = integrity_checkpoint(
            cls, class_outdir, reps_per_class, class_start_iso, checkpoint_path=checkpoint_path
        )
        manifest["checkpoints"].append(checkpoint_result)
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

    stop_driver_log_capture(capture)

    manifest["campaign_end_utc"] = now_iso()
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    ok = sum(1 for r in manifest["runs"] if r["status"] == "ok")
    print(f"\ncampaign done: {ok}/{total} ok, manifest at {manifest_path}")

    print(f"\n=== integrity checkpoint summary ({checkpoint_path}) ===")
    for cp in manifest["checkpoints"]:
        print(f"  {cp['fault_class']}: {cp['status']} "
              f"(records={cp['record_count']}/{cp['expected_reps']}, "
              f"storage={'ok' if cp['storage_ok'] else 'REVERTED'}, "
              f"prom_data={'ok' if cp['prom_ok'] else 'EMPTY'})")

    print(f"\n=== Spark crash-recovery summary: {len(crash_events)} crash(es) detected and auto-healed ===")
    for ev in crash_events:
        print(f"  {ev['detected_utc']}: during {ev['fault_class_active']} rep {ev['rep_lost']}, "
              f"signature_matched={ev['signature_matched']}")

    return manifest


def count_valid_records(class_outdir):
    return len(
        [f for f in os.listdir(class_outdir) if f.endswith(".json")]
    ) if os.path.isdir(class_outdir) else 0


def top_up_class(cls, target_valid, outdir, checkpoint_path=CHECKPOINT_LOG_DEFAULT,
                  min_gap_s=45, max_gap_s=90, max_attempts=None, driver_log_dir=None,
                  manifest_path=None):
    """Keep attempting reps of a single fault class until class_outdir holds
    target_valid ground-truth records, healing Spark on detected crashes rather than
    just recording the loss -- used to top up a class after some reps were lost, e.g.
    executor_oom's 5/8 from the N=8 run, without re-running the whole class from
    scratch. max_attempts bounds runaway attempts if something is systematically
    broken (defaults to target_valid * 3 -- generous, not infinite): a top-up that
    can't reach target within that budget is itself a signal to stop and report, per
    the project's pivot rule, not to keep burning cluster time."""
    runner = FAULT_RUNNERS[cls]
    class_outdir = os.path.join(outdir, cls)
    os.makedirs(class_outdir, exist_ok=True)
    driver_log_dir = driver_log_dir or os.path.join(outdir, "driver_logs")
    max_attempts = max_attempts or target_valid * 3
    manifest_path = manifest_path or os.path.join(outdir, f"topup_{cls}_manifest.json")

    topup_start_iso = now_iso()
    manifest = {"fault_class": cls, "target_valid": target_valid,
                "start_utc": topup_start_iso, "runs": [], "crash_events": []}
    crash_events = manifest["crash_events"]
    capture = ensure_driver_log_capture(None, driver_log_dir) if cls in SPARK_DEPENDENT_CLASSES else None

    starting_valid = count_valid_records(class_outdir)
    print(f"top-up: {cls} currently has {starting_valid} valid record(s), target {target_valid}")

    attempt = 0
    while count_valid_records(class_outdir) < target_valid and attempt < max_attempts:
        attempt += 1
        if cls in SPARK_DEPENDENT_CLASSES and not spark_pods_healthy():
            print(f"  [pre-attempt check] Spark unhealthy before {cls} top-up attempt {attempt} -- healing...")
            handle_spark_crash(cls, f"topup{attempt}", crash_events, checkpoint_path=checkpoint_path)
            capture = ensure_driver_log_capture(capture, driver_log_dir)

        run_id = f"topup{attempt}"
        t0 = time.time()
        status, error = "ok", None
        try:
            runner(run_id, class_outdir)
        except Exception as e:
            status, error = "error", f"{type(e).__name__}: {e}"
            if cls in SPARK_DEPENDENT_CLASSES and not spark_pods_healthy():
                print(f"  [post-attempt check] {cls} top-up attempt {attempt} failed and "
                      f"Spark is unhealthy -- healing...")
                handle_spark_crash(cls, f"topup{attempt}", crash_events, checkpoint_path=checkpoint_path)
                capture = ensure_driver_log_capture(capture, driver_log_dir)
        elapsed = time.time() - t0

        manifest["runs"].append({
            "fault_class": cls, "rep": run_id, "run_id": run_id, "status": status,
            "error": error, "elapsed_seconds": round(elapsed, 1), "started_utc": now_iso(),
        })
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        error_suffix = f" error={error}" if error else ""
        print(f"  top-up attempt {attempt}: status={status} elapsed={elapsed:.1f}s{error_suffix} "
              f"(valid now: {count_valid_records(class_outdir)}/{target_valid})")

        if count_valid_records(class_outdir) < target_valid and attempt < max_attempts:
            gap = random.uniform(min_gap_s, max_gap_s)
            print(f"  cooldown {gap:.0f}s...")
            time.sleep(gap)

    stop_driver_log_capture(capture)

    final_valid = count_valid_records(class_outdir)
    reached = final_valid >= target_valid
    manifest["end_utc"] = now_iso()
    manifest["attempts"] = attempt
    manifest["final_valid"] = final_valid
    manifest["reached_target"] = reached
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"\ntop-up done: {cls} now has {final_valid}/{target_valid} valid record(s) after "
          f"{attempt} attempt(s), {len(crash_events)} crash(es) observed and auto-healed. "
          f"{'TARGET REACHED' if reached else 'TARGET NOT REACHED -- max_attempts exhausted'}")
    for ev in crash_events:
        print(f"  crash: {ev['detected_utc']} during {cls} attempt {ev['rep_lost']}, "
              f"signature_matched={ev['signature_matched']}")

    # Fresh checkpoint covering the full, now-topped-up set of valid records (not just
    # this call's new attempts) -- record_count/timing scan class_outdir's entire
    # contents regardless of window; the prometheus check's window is scoped to this
    # top-up call's own run time.
    checkpoint_result = integrity_checkpoint(
        cls, class_outdir, target_valid, topup_start_iso, checkpoint_path=checkpoint_path
    )
    manifest["checkpoint"] = checkpoint_result
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reps", type=int, default=15, help="repetitions per fault class")
    parser.add_argument("--classes", nargs="*", default=None,
                         help="subset of fault classes to run (default: all 5)")
    parser.add_argument("--top-up-class", default=None,
                         help="instead of a full campaign, keep attempting this one fault "
                              "class until --top-up-target valid records exist")
    parser.add_argument("--top-up-target", type=int, default=8)
    parser.add_argument("--min-gap-s", type=int, default=45)
    parser.add_argument("--max-gap-s", type=int, default=90)
    parser.add_argument("--outdir", default="results/campaign")
    parser.add_argument("--skip-preflight", action="store_true")
    parser.add_argument("--checkpoint-path", default=CHECKPOINT_LOG_DEFAULT)
    args = parser.parse_args()
    if args.top_up_class:
        top_up_class(args.top_up_class, args.top_up_target, args.outdir,
                     checkpoint_path=args.checkpoint_path,
                     min_gap_s=args.min_gap_s, max_gap_s=args.max_gap_s)
    else:
        run_campaign(args.reps, classes=args.classes, min_gap_s=args.min_gap_s,
                     max_gap_s=args.max_gap_s, outdir=args.outdir, skip_preflight=args.skip_preflight,
                     checkpoint_path=args.checkpoint_path)
