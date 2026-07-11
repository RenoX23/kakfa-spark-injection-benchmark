"""Forced, isolated validation of campaign.py's Spark crash-recovery wrapper
(heal_spark/handle_spark_crash), run in isolation from real campaign data.

Context: the 2026-07-11 N=8 campaign hit "[INTERNAL_ERROR] The Spark SQL phase
optimization failed with an internal error" twice, both during executor_oom activity.
Root cause was never identified (the crashed driver pods' logs and the k8s events for
that window were already gone by the time anyone went looking). campaign.py grew a
detect -> capture -> heal -> log wrapper in response, but that wrapper had only been
smoke-tested against a healthy cluster -- never exercised against a real crash. This
script hammers executor_oom with a much shorter gap than a real campaign rep (aiming to
reproduce the race/timing-sensitive crash faster) and, if it reproduces, runs the
*actual* production healer code path (not a reimplementation) through staged checks.

Detection is deliberately gated on DRIVER phase specifically, checked independently via
kubectl, not campaign.spark_pods_healthy()'s blanket driver-AND-executor check:
executor_oom.run() already polls up to 120s internally for the replacement executor to
reach Running before it returns, so a blanket health check right after could
false-positive on ordinary executor respawn lag instead of the actual target bug (a
driver-process-level crash). Only "driver pod missing or not Running" counts as the
crash signature this script is trying to reproduce.

Writes nothing into results/campaign-n8/ -- this is an infrastructure test, not
campaign data. Everything lives under results/healer-validation/.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from common import kubectl, now_iso
import campaign
import executor_oom

OUTDIR = "results/healer-validation"
CLASS_OUTDIR = os.path.join(OUTDIR, "executor_oom")
DRIVER_LOG_DIR = os.path.join(OUTDIR, "driver_logs")
CHECKPOINT_PATH = os.path.join(OUTDIR, "crash_recovery_validation.log")
REPORT_PATH = os.path.join(OUTDIR, "validation_report.json")


def _driver_state(namespace="spark"):
    """(name, uid, phase) of the current driver pod. Independent of
    campaign.spark_pods_healthy() -- this script's whole point is to check the healer,
    so it shouldn't lean on the same helper the healer itself uses for its own trigger."""
    out = kubectl(
        "-n", namespace, "get", "pods", "-l", "spark-role=driver",
        "-o", 'jsonpath={.items[0].metadata.name}{"\\t"}{.items[0].metadata.uid}{"\\t"}{.items[0].status.phase}',
        check=False,
    ).stdout.strip()
    parts = out.split("\t")
    if len(parts) != 3 or not parts[0]:
        return None, None, "MISSING"
    return parts[0], parts[1], parts[2]


def run(max_attempts=8, gap_s=15):
    os.makedirs(CLASS_OUTDIR, exist_ok=True)
    print(f"=== healer forced-validation: up to {max_attempts} rapid executor_oom attempts, "
          f"gap={gap_s}s (normal campaign gap is 45-90s) ===")

    campaign.preflight_check()
    name0, uid0, phase0 = _driver_state()
    if phase0 != "Running":
        print(f"driver not healthy before starting ({phase0}) -- healing first, "
              f"out-of-band, not counted as part of the validation")
        campaign.heal_spark()

    capture = campaign.ensure_driver_log_capture(None, DRIVER_LOG_DIR)

    attempts_log = []
    crash_confirmed = False
    stages = {}

    for attempt in range(1, max_attempts + 1):
        run_id = f"healercheck{attempt}"
        t0 = time.time()
        try:
            executor_oom.run(run_id, outdir=CLASS_OUTDIR)
            status = "ok"
        except Exception as e:
            status = f"error: {type(e).__name__}: {e}"
        elapsed = time.time() - t0

        name, uid, phase = _driver_state()
        driver_crashed = (phase != "Running")
        attempts_log.append({
            "attempt": attempt, "status": status, "elapsed_s": round(elapsed, 1),
            "driver_phase_after": phase, "driver_crashed": driver_crashed,
        })
        print(f"[{attempt}/{max_attempts}] executor_oom: {status} ({elapsed:.1f}s) "
              f"-- driver phase after: {phase}")

        if driver_crashed:
            crash_confirmed = True
            print(f"*** driver phase is '{phase}' (not Running) after attempt {attempt} "
                  f"-- crash signature reproduced, running staged validation ***")
            stages = _staged_validation(attempt, name, uid, capture)
            break

        if attempt < max_attempts:
            time.sleep(gap_s)

    report = {
        "started_utc": now_iso(), "max_attempts": max_attempts, "gap_s": gap_s,
        "attempts": attempts_log, "crash_reproduced": crash_confirmed, "stages": stages,
    }

    if not crash_confirmed:
        print(f"\nRESULT: crash did NOT reproduce in {max_attempts} rapid attempts "
              f"(gap={gap_s}s). heal_spark()/handle_spark_crash() remain unexercised "
              f"against a real crash. This is documented residual risk, not a failure "
              f"of this test -- not extending further per instruction.")
    else:
        all_pass = all(v is True for v in stages.values())
        print(f"\nRESULT: crash reproduced on attempt {[a['attempt'] for a in attempts_log if a['driver_crashed']][0]}. "
              f"Staged validation: {'ALL STAGES PASS' if all_pass else 'AT LEAST ONE STAGE FAILED -- see above'}")

    os.makedirs(OUTDIR, exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nfull report: {REPORT_PATH}")
    return report


def _staged_validation(attempt, crashed_name, crashed_uid, capture):
    results = {}

    # Stage 1: detection -- independently re-verify (not just trust the loop's own
    # driver_crashed flag) that the driver really is down right now.
    _, _, phase_check = _driver_state()
    results["stage1_detection"] = phase_check != "Running"
    print(f"  STAGE 1 (detection): {'PASS' if results['stage1_detection'] else 'FAIL'} "
          f"-- independent re-check of driver phase: {phase_check}")

    # Snapshot the persistent capture file's size BEFORE healing deletes the crashed pod.
    pre_heal_size = os.path.getsize(capture["path"]) if capture and os.path.exists(capture["path"]) else None
    pre_heal_content = ""
    if capture and os.path.exists(capture["path"]):
        with open(capture["path"]) as f:
            pre_heal_content = f.read()

    # Run the REAL production healer path, not a reimplementation.
    crash_events = []
    campaign.handle_spark_crash("executor_oom", f"healercheck{attempt}", crash_events,
                                 checkpoint_path=CHECKPOINT_PATH)

    # Stage 2: did the persistent capture file survive the pod deletion inside
    # handle_spark_crash, with its content intact (not truncated/reset)?
    post_heal_exists = capture and os.path.exists(capture["path"])
    post_heal_size = os.path.getsize(capture["path"]) if post_heal_exists else None
    content_intact = post_heal_exists and pre_heal_content and pre_heal_content in open(capture["path"]).read()
    results["stage2_log_survival"] = bool(post_heal_exists and content_intact)
    print(f"  STAGE 2 (log capture survives deletion): {'PASS' if results['stage2_log_survival'] else 'FAIL'} "
          f"-- file={capture['path'] if capture else None}, "
          f"pre_heal_size={pre_heal_size}, post_heal_size={post_heal_size}, "
          f"pre-heal content still a prefix of post-heal file={content_intact}")

    # Stage 3: heal_spark() (called inside handle_spark_crash) actually deleted and
    # recreated the driver -- proven by UID change, not just phase reading Running again
    # (a UID match with Running would mean it never actually left, i.e. a no-op heal).
    post_name, post_uid, post_phase = _driver_state()
    results["stage3_recreate"] = post_uid is not None and post_uid != crashed_uid
    print(f"  STAGE 3 (delete+recreate): {'PASS' if results['stage3_recreate'] else 'FAIL'} "
          f"-- crashed pod uid={crashed_uid}, post-heal pod uid={post_uid} "
          f"(name {crashed_name} -> {post_name})")

    # Stage 4: health check confirms recovery (re-verify independently; heal_spark
    # itself already polled for this internally and would have raised if it failed, but
    # confirm it explicitly rather than inferring from "no exception was raised").
    results["stage4_recovery_confirmed"] = post_phase == "Running" and campaign.spark_pods_healthy()
    print(f"  STAGE 4 (recovery confirmed): {'PASS' if results['stage4_recovery_confirmed'] else 'FAIL'} "
          f"-- driver phase={post_phase}, spark_pods_healthy()={campaign.spark_pods_healthy()}")

    # Stage 5: checkpoint log actually got a real, well-formed entry, and the in-memory
    # crash_events list (what a real campaign run would fold into its manifest) has
    # exactly the one event.
    checkpoint_text = open(CHECKPOINT_PATH).read() if os.path.exists(CHECKPOINT_PATH) else ""
    results["stage5_checkpoint_logged"] = (
        len(crash_events) == 1
        and "SPARK CRASH RECOVERY" in checkpoint_text
        and crash_events[0]["fault_class_active"] == "executor_oom"
    )
    print(f"  STAGE 5 (checkpoint log entry): {'PASS' if results['stage5_checkpoint_logged'] else 'FAIL'} "
          f"-- crash_events recorded={len(crash_events)}, "
          f"checkpoint file contains block={'SPARK CRASH RECOVERY' in checkpoint_text}")
    if crash_events:
        print(f"    signature_matched ('{campaign.SPARK_CRASH_SIGNATURE}'): {crash_events[0]['signature_matched']}")

    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-attempts", type=int, default=8)
    parser.add_argument("--gap-s", type=int, default=15)
    args = parser.parse_args()
    run(max_attempts=args.max_attempts, gap_s=args.gap_s)
