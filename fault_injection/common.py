"""Shared plumbing for fault-injection scripts (Weeks 2-3, Section 6.2 locked taxonomy).

Deliberately stdlib-only (subprocess/json/urllib) -- these are thin wrappers around
kubectl and Prometheus's HTTP API, not enough surface to justify adding SDK
dependencies (kubernetes client, requests) yet. Revisit if that changes.
"""
import datetime
import json
import socket
import subprocess
import time
import urllib.parse
import urllib.request


def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def kubectl(*args, check=True, capture=True):
    return subprocess.run(
        ["kubectl", *args], capture_output=capture, text=True, check=check
    )


def free_local_port():
    """Pick an OS-assigned free port instead of hardcoding one. Hardcoding 9090 broke
    the first run of this script: a leftover manual port-forward from earlier in the
    session was already bound to it, and the collision surfaced as a confusing
    'port-forward exited early' error with no clear cause. Repeated automated runs
    (N>=15-20 per fault class) can't depend on no other process ever touching a fixed
    port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


def start_port_forward(namespace, target, remote_port, timeout=15):
    """target: e.g. 'pod/kspfail-single-0' or 'svc/prometheus-server'.
    Returns (Popen handle, local_port actually bound)."""
    local_port = free_local_port()
    proc = subprocess.Popen(
        ["kubectl", "-n", namespace, "port-forward", target, f"{local_port}:{remote_port}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = proc.stdout.readline()
        if "Forwarding from" in line:
            return proc, local_port
        if proc.poll() is not None:
            raise RuntimeError(f"port-forward exited early: {line}")
    proc.terminate()
    raise TimeoutError(f"port-forward to {target} did not become ready within {timeout}s")


def stop_port_forward(proc):
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def prom_query(query, port):
    url = f"http://localhost:{port}/api/v1/query?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.load(resp)


def prom_query_range(query, start_iso, end_iso, step, port):
    params = {"query": query, "start": start_iso, "end": end_iso, "step": step}
    url = f"http://localhost:{port}/api/v1/query_range?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=15) as resp:
        return json.load(resp)


def prom_target_health(scrape_pool, port):
    url = f"http://localhost:{port}/api/v1/targets"
    with urllib.request.urlopen(url, timeout=10) as resp:
        data = json.load(resp)
    for t in data["data"]["activeTargets"]:
        if t["scrapePool"] == scrape_pool:
            return t["health"]
    return "missing"


def poll_until(check_fn, timeout=120, interval=3):
    """Returns the ISO timestamp of the first True result, or None on timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if check_fn():
            return now_iso()
        time.sleep(interval)
    return None


def write_ground_truth(record, path):
    with open(path, "w") as f:
        json.dump(record, f, indent=2)
    return path
