"""Fault class: network degradation (latency/packet-loss). Section 6.2, locked taxonomy.

The Strimzi Kafka image has no `tc`/iproute2 at all (checked directly: `which tc` fails
with "executable file not found"). Standard fix: an ephemeral debug container
(nicolaka/netshoot, the common Kubernetes network-troubleshooting image) attached to the
target pod with `--target` (shares its network namespace) and Kubernetes's built-in
`netadmin` debug profile (grants CAP_NET_ADMIN to the ephemeral container specifically,
not the broker's own container -- keeps the broker's security posture unchanged).

Injection and removal happen in a SINGLE ephemeral-container invocation (add netem ->
sleep for the fault duration -> remove netem), not two separate `kubectl debug` calls --
`kubectl debug`'s attach/log retrieval has a real race where a second invocation's logs
can become unavailable moments after the container exits, so splitting inject/remove
across two sessions risked never confirming removal happened.

Ground truth has two parts, not one:
- Binary target health (up/down) -- a coarse signal. First run at 300ms delay / 10% loss
  never flipped it: Prometheus's scrape_timeout is generous enough (10s default) to
  absorb that level of degradation without an outright scrape failure. That's a real,
  useful finding (degradation is by definition often sub-critical), not a bug -- kept as
  the default severity, with a documented, deliberately harsher --severe option for a
  run that does cross the binary threshold.
- scrape_duration_seconds -- a continuous, more sensitive signal that should show a real
  increase even when the scrape still nominally succeeds.

Removal verification does NOT parse the debug session's captured stdout (unreliable --
first attempt showed a clean exitCode=0 in the K8s-reported ephemeral container status
while the locally-captured stdout string match still came back false). It queries the pod's
ephemeralContainerStatuses directly after the process completes and checks the
Kubernetes-reported exit code -- the authoritative source, not a side channel.
"""
import argparse
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(__file__))
from common import (
    kubectl,
    now_iso,
    poll_until,
    prom_query,
    prom_target_health,
    start_port_forward,
    stop_port_forward,
    write_ground_truth,
)

FAULT_CLASS = "network_degradation"


def latest_ephemeral_exit_code(namespace, pod, after_iso_ts):
    import datetime
    after = datetime.datetime.strptime(after_iso_ts, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=datetime.timezone.utc
    )
    result = kubectl("-n", namespace, "get", "pod", pod, "-o", "json", check=False)
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    candidates = []
    for c in data.get("status", {}).get("ephemeralContainerStatuses", []):
        term = c.get("state", {}).get("terminated")
        if not term:
            continue
        started = datetime.datetime.strptime(
            term["startedAt"], "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=datetime.timezone.utc)
        if started >= after:
            candidates.append((started, term["exitCode"]))
    if not candidates:
        return None
    candidates.sort()
    return candidates[0][1]


def scrape_duration(scrape_pool, port):
    result = prom_query(f'scrape_duration_seconds{{job="{scrape_pool}"}}', port)
    res = result["data"]["result"]
    return float(res[0]["value"][1]) if res else None


def run(run_id, namespace="kafka", pod="kspfail-single-0", scrape_pool="kafka-broker-jmx",
        interface="eth0", delay_ms=300, jitter_ms=50, loss_pct=10, duration_s=30,
        outdir="results/fault-runs"):
    os.makedirs(outdir, exist_ok=True)
    pf, port = start_port_forward("monitoring", "svc/prometheus-server", 80)
    try:
        baseline_health = prom_target_health(scrape_pool, port)
        if baseline_health != "up":
            raise RuntimeError(f"refusing to inject: target health is '{baseline_health}', not 'up' before injection")
        baseline_scrape_duration = scrape_duration(scrape_pool, port)

        inject_ts = now_iso()
        script = (
            f"tc qdisc add dev {interface} root netem delay {delay_ms}ms {jitter_ms}ms loss {loss_pct}% && "
            f"echo NETEM_APPLIED && sleep {duration_s} && "
            f"tc qdisc del dev {interface} root && echo NETEM_REMOVED"
        )
        # subprocess.Popen, not the blocking kubectl() wrapper: kubectl debug's own
        # `sleep {duration_s}` inside the script means a blocking call wouldn't return
        # until AFTER netem was already removed, missing the entire degradation window
        # this script exists to observe. Run it in the background, poll for degradation
        # while it's still active, then reap the process at the end.
        proc = subprocess.Popen(
            [
                "kubectl", "-n", namespace, "debug", pod,
                "--image=nicolaka/netshoot", "--target=kafka", "--profile=netadmin",
                "--", "sh", "-c", script,
            ],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )

        degraded_detected_ts = poll_until(
            lambda: prom_target_health(scrape_pool, port) != "up",
            timeout=max(duration_s - 5, 5),
            interval=2,
        )
        peak_scrape_duration = max(
            (d for d in [scrape_duration(scrape_pool, port) for _ in range(3)] if d is not None),
            default=None,
        )

        proc.communicate(timeout=60)
        # The K8s API's ephemeralContainerStatuses lags slightly behind the container
        # actually exiting (a kubelet-to-apiserver status propagation delay) -- an
        # immediate single check here came back null even though the container had
        # genuinely already exited 0; a short poll instead of one-shot fixes it.
        exit_code_holder = {}
        poll_until(
            lambda: (exit_code_holder.__setitem__(
                "v", latest_ephemeral_exit_code(namespace, pod, inject_ts)
            ) or exit_code_holder["v"] is not None),
            timeout=20,
            interval=2,
        )
        removal_exit_code = exit_code_holder.get("v")

        recovered_ts = poll_until(
            lambda: prom_target_health(scrape_pool, port) == "up",
            timeout=60,
            interval=3,
        )

        record = {
            "fault_class": FAULT_CLASS,
            "run_id": run_id,
            "namespace": namespace,
            "target_pod": pod,
            "interface": interface,
            "delay_ms": delay_ms,
            "jitter_ms": jitter_ms,
            "loss_pct": loss_pct,
            "duration_s": duration_s,
            "injection_timestamp_utc": inject_ts,
            "netem_removal_exit_code": removal_exit_code,
            "removal_confirmed": removal_exit_code == 0,
            "baseline_scrape_duration_seconds": baseline_scrape_duration,
            "peak_scrape_duration_seconds_during_fault": peak_scrape_duration,
            "degraded_health_detected_utc": degraded_detected_ts,
            "target_recovered_utc": recovered_ts,
            "recovered": recovered_ts is not None,
        }
        path = write_ground_truth(record, os.path.join(outdir, f"{FAULT_CLASS}_run{run_id}.json"))
        print(
            f"wrote {path}: removal_exit_code={removal_exit_code}, "
            f"scrape_duration {baseline_scrape_duration} -> {peak_scrape_duration}, "
            f"health_flipped={degraded_detected_ts is not None}"
        )
        return record
    finally:
        stop_port_forward(pf)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--duration-s", type=int, default=30)
    parser.add_argument("--delay-ms", type=int, default=300)
    parser.add_argument("--loss-pct", type=int, default=10)
    args = parser.parse_args()
    run(args.run_id, duration_s=args.duration_s, delay_ms=args.delay_ms, loss_pct=args.loss_pct)
