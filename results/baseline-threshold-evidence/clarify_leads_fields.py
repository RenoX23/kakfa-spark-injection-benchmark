"""One-time provenance fix for evaluation_output.json (2026-07-13), triggered by a real
discrepancy found while drafting the paper's Results section: the top-level `leads`
field means a DIFFERENT thing per class, and for disk_pressure/network_degradation it
is stale, superseded data that was never removed after the severity-threshold redefinition
gate-audit required (docs/baseline_thresholds.md, "Full Evaluation Results" section,
lines 341-346: "It previously measured crossing->fault-natural-end... it now measures
crossing->severity-threshold").

Root cause, traced directly from results/baseline-threshold-evidence/evaluate_baselines.py:

- **executor_oom**: `leads[i] = oom_confirmed_ts - crossing_ts`. `oom` IS the real crash
  event (the OOM kill itself) -- this field was always correct, matches
  docs/baseline_thresholds.md Section 2's explicit "lead time (threshold crossing -> OOM)"
  definition, and needs no numeric correction. Renamed only, for a clear, symmetric name
  across all four classes.
- **broker_kill**: `leads[i] = target_recovered_ts - crossing_ts`. This is NOT a
  crossing-to-crash-event lead time -- for a binary up/down signal, "crossing" (up first
  reads 0) already IS witnessing the outage, not a precursor to it. What this field
  actually measures is the residual outage duration AFTER detection, i.e.
  crossing-to-recovery. It is not stale (nothing superseded it) and not comparable in
  kind to the other three classes' crossing-to-failure/severity lead times. Renamed to
  make that explicit; NOT renumbered as a "lead time" implying predictive early warning,
  and flagged in the output for the paper draft's attention -- whether this quantity
  belongs in a "baseline lead time" column at all is a methodological question this
  script deliberately does not decide.
- **disk_pressure / network_degradation**: `leads[i] = natural_end_ts - crossing_ts`.
  This is the OLD, pre-gate-audit-fix definition (crossing -> the fault simply stopping,
  not a failure) -- confirmed stale by direct comparison: recomputing crossing-to-
  severity from these two classes' own `severity_detail` field (the field
  docs/research_context.md's Section 6.5 tables actually cite) reproduces the reported
  numbers exactly (disk_pressure: 0.0s, 8/8; network_degradation: 0s x3, 65s x1, 4/8
  reach severity) -- `severity_detail` was already correct and already the reported
  source; the plain `leads` array sitting next to it was simply never removed.

Fix: rename ambiguous fields to unambiguous ones for broker_kill/executor_oom: keep the
exact same values, only the key changes. For disk_pressure/network_degradation: move the
stale array under a clearly-labeled `_SUPERSEDED` key (kept, not deleted, per this
project's own established convention of retaining superseded numbers as disclosed
history rather than erasing them -- e.g. the window/horizon config history in
docs/research_context.md Section 6.4) rather than silently dropping historical data from
a committed evidence file.
"""
import json
from pathlib import Path

PATH = Path(__file__).resolve().parent / "evaluation_output.json"


def main():
    d = json.loads(PATH.read_text())

    # broker_kill: leads = crossing -> recovery, NOT crossing -> crash-event.
    bk = d["broker_kill"]
    bk["crossing_to_recovery_s"] = bk.pop("leads")
    bk["_note"] = (
        "This is crossing (up==0 first observed) -> target_recovered_utc, i.e. residual "
        "outage duration AFTER detection -- NOT comparable to executor_oom's "
        "crossing_to_crash_event_s or disk_pressure/network_degradation's "
        "crossing_to_severity_lead_s (both measure time BEFORE a worse outcome). For a "
        "binary up/down signal, crossing already IS witnessing the outage, so there is "
        "no distinct earlier 'crash event' for this class to lead into. Whether this "
        "value belongs in a 'baseline lead time' comparison at all is a methodological "
        "question this fix does not resolve on its own authority -- see "
        "docs/research_context.md's provenance note for the same finding."
    )

    # executor_oom: leads = crossing -> OOM (the real crash event). Correct already,
    # renamed only for a clear, symmetric field name across all four classes.
    eo = d["executor_oom"]
    eo["crossing_to_crash_event_s"] = eo.pop("leads")

    # disk_pressure / network_degradation: leads = crossing -> natural_end (pre-fix,
    # stale). severity_detail's crossing_to_severity_lead_s is the correct, currently-
    # reported metric and needs no change -- confirmed by direct recomputation (see
    # docstring). Move the stale array aside, don't delete it.
    for cls in ("disk_pressure", "network_degradation"):
        entry = d[cls]
        entry["leads_crossing_to_natural_end_s_SUPERSEDED"] = entry.pop("leads")
        entry["_note"] = (
            "This SUPERSEDED field measures crossing -> fault-natural-end (the fault "
            "simply stopping), the pre-gate-audit-fix definition -- see "
            "docs/baseline_thresholds.md 'Full Evaluation Results' section. The current, "
            "reported metric is severity_detail[*].crossing_to_severity_lead_s, "
            "unchanged and not stale. Kept here as disclosed history, not deleted."
        )

    PATH.write_text(json.dumps(d, indent=2))
    print(f"wrote {PATH}")

    print("\n=== verification: recomputed values vs. docs/research_context.md Section 6.5 ===")
    print(f"broker_kill.crossing_to_recovery_s = {bk['crossing_to_recovery_s']} "
          f"(doc reports 57.0s, n=1)")
    print(f"executor_oom.crossing_to_crash_event_s: n={len(eo['crossing_to_crash_event_s'])}, "
          f"mean={sum(eo['crossing_to_crash_event_s'])/len(eo['crossing_to_crash_event_s']):.3f} "
          f"(doc reports 64.9s mean, range 48-83s, n=8/8)")
    for cls in ("disk_pressure", "network_degradation"):
        sd = d[cls]["severity_detail"]
        reached = [r for r in sd if r["severity_reached"]]
        leads = [r["crossing_to_severity_lead_s"] for r in reached]
        print(f"{cls}.severity_detail: n_reached={len(reached)}/{len(sd)}, leads={leads}")


if __name__ == "__main__":
    main()
