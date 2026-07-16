import subprocess, time, urllib.request, urllib.parse, json
from datetime import datetime, timedelta

pf = subprocess.Popen(['kubectl','-n','monitoring','port-forward','svc/prometheus-server','19191:80'],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(3)
BASE = 'http://localhost:19191/api/v1/query_range'

def q(query, start, end, step='15s'):
    params = urllib.parse.urlencode({'query': query, 'start': start, 'end': end, 'step': step})
    req = urllib.request.urlopen(f'{BASE}?{params}', timeout=20)
    d = json.loads(req.read())
    return d.get('data', {}).get('result', [])

def iso(s):
    return datetime.fromisoformat(s.replace('Z', '+00:00'))

def add(s, seconds):
    return (iso(s) + timedelta(seconds=seconds)).isoformat().replace('+00:00', 'Z')

# BUG CAUGHT DURING FIRST RUN OF THIS SCRIPT, FIXED HERE: a naive gap check (query from
# a rep's exact recovery/target_recovered_utc timestamp to the next rep's injection) picks
# up Prometheus's range-query step-interpolation carrying that rep's own last REAL in-fault
# sample forward past the recovery boundary -- not a genuine post-recovery reading. First
# run showed network_degradation "false-firing" in all 7 gap windows; the values were
# exactly each rep's own recorded peak_scrape_duration_seconds. Fixed by padding every gap
# start by one scrape interval (65s, with margin) past the recovery boundary before
# checking for crossings. Applied uniformly to every gap-based FPR check below (broker_kill,
# disk_pressure, network_degradation) -- re-verified broker_kill/disk_pressure's
# already-clean results under this same fix too, to rule out them being clean by the same
# wrong method rather than genuinely clean. Both held up. executor_oom's FPR check doesn't
# need this: it queries pre-injection windows scoped to each rep's own distinct pod name,
# a structurally separate metric series with no possible carryover from a different pod.
def check_gap_fpr(query_str, gap_start, gap_end, crosses_fn, pad_s=65):
    padded_start = add(gap_start, pad_s)
    if iso(padded_start) >= iso(gap_end):
        return None  # gap too short in real time to check after padding
    series = q(query_str, padded_start, gap_end, '5s')
    return crosses_fn(series)

results = {}

# ---------------- broker_kill: up == 0 ----------------
broker_reps = [
    ('campaign1','2026-07-11T10:24:30Z','2026-07-11T10:25:25Z'),
    ('campaign2','2026-07-11T10:26:47Z','2026-07-11T10:27:01Z'),
    ('campaign3','2026-07-11T10:27:56Z','2026-07-11T10:28:58Z'),
    ('campaign4','2026-07-11T10:29:56Z','2026-07-11T10:30:09Z'),
    ('campaign5','2026-07-11T10:30:56Z','2026-07-11T10:31:55Z'),
    ('campaign6','2026-07-11T10:32:54Z','2026-07-11T10:33:29Z'),
    ('campaign7','2026-07-11T10:34:59Z','2026-07-11T10:35:10Z'),
    ('campaign8','2026-07-11T10:36:33Z','2026-07-11T10:37:04Z'),
]
tp, fn, leads = 0, 0, []
for rid, inj, rec in broker_reps:
    series = q('up{job="kafka-broker-jmx"}', inj, rec, '5s')
    vals = [(int(t), float(v)) for t, v in series[0]['values']] if series else []
    zero = next((t for t, v in vals if v == 0), None)
    if zero:
        tp += 1
        leads.append(iso(rec).timestamp() - zero)
    else:
        fn += 1
# FPR: inter-rep gaps (clean steady-state, no discarded contamination for this class),
# padded to skip the stale in-fault carryover artifact (see check_gap_fpr docstring above)
gaps = [(broker_reps[i][2], broker_reps[i+1][1]) for i in range(len(broker_reps)-1)]
fp_windows, checked, too_short = 0, 0, 0
for g_start, g_end in gaps:
    r = check_gap_fpr('up{job="kafka-broker-jmx"}', g_start, g_end,
                       lambda series: any(float(v) == 0 for s in series for _, v in s['values']))
    if r is None:
        too_short += 1
    else:
        checked += 1
        fp_windows += int(r)
# Named crossing_to_recovery_s, not "leads" -- this is crossing (up==0 first observed) to
# target_recovered_utc, i.e. residual outage duration AFTER detection, not a
# crossing-to-crash-event lead time like executor_oom's. For a binary up/down signal,
# crossing already IS witnessing the outage; there's no distinct earlier "crash event"
# for this class to lead into. Found ambiguous and fixed 2026-07-13 -- see
# results/baseline-threshold-evidence/clarify_leads_fields.py for the full trace.
results['broker_kill'] = dict(tp=tp, fn=fn, fp=fp_windows, gaps_checked=checked, gaps_too_short=too_short, crossing_to_recovery_s=leads)

# ---------------- executor_oom: working_set > 900MB ----------------
oom_reps = [
    ('ramptest3','kspfail-streaming-pilot-5f2d2b9f520ff974-exec-1','2026-07-11T17:13:37Z','2026-07-11T17:17:06Z'),
    ('ramptest4','kspfail-streaming-pilot-5f2d2b9f520ff974-exec-2','2026-07-11T17:17:38Z','2026-07-11T17:21:28Z'),
    ('ramptest5','kspfail-streaming-pilot-5f2d2b9f520ff974-exec-3','2026-07-11T17:22:01Z','2026-07-11T17:25:54Z'),
    ('ramptest6','kspfail-streaming-pilot-5f2d2b9f520ff974-exec-4','2026-07-11T17:26:27Z','2026-07-11T17:30:15Z'),
    ('ramptest7','kspfail-streaming-pilot-5f2d2b9f520ff974-exec-5','2026-07-11T17:30:48Z','2026-07-11T17:34:37Z'),
    ('ramptest8','kspfail-streaming-pilot-5f2d2b9f520ff974-exec-6','2026-07-11T17:35:09Z','2026-07-11T17:38:59Z'),
    ('ramptest9','kspfail-streaming-pilot-5f2d2b9f520ff974-exec-7','2026-07-11T17:39:32Z','2026-07-11T17:43:19Z'),
    ('ramptest10','kspfail-streaming-pilot-5f2d2b9f520ff974-exec-8','2026-07-11T17:45:41Z','2026-07-11T17:49:24Z'),
]
THRESH_MEM = 900_000_000
tp, fn, leads = 0, 0, []
for rid, pod, inj, oom in oom_reps:
    series = q(f'container_memory_working_set_bytes{{pod="{pod}", container="spark-kubernetes-executor"}}', inj, oom, '15s')
    vals = [(int(t), float(v)) for t, v in series[0]['values']] if series else []
    crossing = next((t for t, v in vals if v > THRESH_MEM), None)
    if crossing:
        tp += 1
        leads.append(iso(oom).timestamp() - crossing)
    else:
        fn += 1
# FPR: pre-injection 90s baseline windows for each rep's own pod (settled/organic memory, no fault active yet)
fp_windows = 0
for rid, pod, inj, oom in oom_reps:
    start = add(inj, -90)
    series = q(f'container_memory_working_set_bytes{{pod="{pod}", container="spark-kubernetes-executor"}}', start, inj, '15s')
    vals = [float(v) for s in series for _, v in s['values']]
    if any(v > THRESH_MEM for v in vals):
        fp_windows += 1
# Named crossing_to_crash_event_s: OOM-confirmed is the real crash event for this class,
# so crossing-to-OOM is a genuine predictive lead time, unlike broker_kill's
# crossing_to_recovery_s above. Matches docs/baseline_thresholds.md Section 2's explicit
# "lead time (threshold crossing -> OOM)" definition -- renamed for a symmetric,
# unambiguous name across all four classes, not because the value itself was wrong.
results['executor_oom'] = dict(tp=tp, fn=fn, fp=fp_windows, gaps_checked=len(oom_reps), gaps_too_short=0, crossing_to_crash_event_s=leads)

# ---------------- disk_pressure: avail drop > 1.5GB from rolling baseline ----------------
disk_reps = [
    ('campaign1','2026-07-11T11:00:18Z','2026-07-11T11:02:14Z'),
    ('campaign2','2026-07-11T11:03:03Z','2026-07-11T11:04:15Z'),
    ('campaign3','2026-07-11T11:05:39Z','2026-07-11T11:07:15Z'),
    ('campaign4','2026-07-11T11:08:28Z','2026-07-11T11:10:15Z'),
    ('campaign5','2026-07-11T11:11:14Z','2026-07-11T11:13:15Z'),
    ('campaign7','2026-07-11T11:17:29Z','2026-07-11T11:19:15Z'),
    ('campaign8','2026-07-11T11:20:25Z','2026-07-11T11:22:16Z'),
    ('topup1','2026-07-11T14:33:37Z','2026-07-11T14:34:56Z'),
]
THRESH_DROP = 1_500_000_000
# Severity-target: 90% of the mean real observed drop across all 8 reps (3,221,544,960 B
# mean -> 2,899,390,464 B), not a round number -- see docs/baseline_thresholds.md for the
# full justification (gate-auditor flagged crossing->recovery lead time as not comparable
# in kind to executor_oom/broker_kill's crossing->crash-event lead time; this is the fix).
SEVERITY_DROP = 2_899_390_464
tp, fn, leads = 0, 0, []
severity_detail = []
for rid, inj, rec in disk_reps:
    start = add(inj, -30)
    series = q('node_filesystem_avail_bytes{mountpoint="/var"}', start, rec, '5s')
    vals = [(int(t), float(v)) for t, v in series[0]['values']] if series else []
    if not vals:
        fn += 1
        continue
    baseline = vals[0][1]
    crossing = next((t for t, v in vals if (baseline - v) > THRESH_DROP), None)
    severity_ts = next((t for t, v in vals if (baseline - v) > SEVERITY_DROP), None)
    if crossing:
        tp += 1
        leads.append(iso(rec).timestamp() - crossing)
    else:
        fn += 1
    severity_detail.append(dict(
        run_id=rid, crossing_utc=(iso('1970-01-01T00:00:00Z')+timedelta(seconds=crossing)).isoformat() if crossing else None,
        severity_utc=(iso('1970-01-01T00:00:00Z')+timedelta(seconds=severity_ts)).isoformat() if severity_ts else None,
        natural_end_utc=rec,
        crossing_to_severity_lead_s=(severity_ts - crossing) if (crossing and severity_ts) else None,
        severity_reached=severity_ts is not None,
    ))
# FPR: clean inter-rep gaps, excluding the one contaminated by discarded campaign6's real
# injection (11:13:15-11:17:29), padded to skip the stale in-fault carryover artifact
disk_gaps_all = [(disk_reps[i][2], disk_reps[i+1][1]) for i in range(len(disk_reps)-1)]
disk_gaps = [g for g in disk_gaps_all if g != ('2026-07-11T11:13:15Z','2026-07-11T11:17:29Z')]
fp_windows, checked, too_short = 0, 0, 0
def disk_crosses(series):
    vals = [(int(t), float(v)) for s in series for t, v in s['values']]
    if not vals:
        return False
    baseline = vals[0][1]
    return any((baseline - v) > THRESH_DROP for _, v in vals)
for g_start, g_end in disk_gaps:
    r = check_gap_fpr('node_filesystem_avail_bytes{mountpoint="/var"}', g_start, g_end, disk_crosses)
    if r is None:
        too_short += 1
    else:
        checked += 1
        fp_windows += int(r)
# `leads` here is crossing -> natural_end (the fault simply stopping), the pre-gate-
# audit-fix definition -- SUPERSEDED by severity_detail's own crossing_to_severity_lead_s,
# which is the metric actually reported in docs/research_context.md. Kept, clearly
# labeled, not silently dropped or left ambiguously named next to the current metric --
# see results/baseline-threshold-evidence/clarify_leads_fields.py for the full trace.
results['disk_pressure'] = dict(tp=tp, fn=fn, fp=fp_windows, gaps_checked=checked, gaps_too_short=too_short, leads_crossing_to_natural_end_s_SUPERSEDED=leads, excluded_gap='campaign6-contaminated (real fill occurred, discarded for unrelated detection-timing reason)', severity_threshold_bytes=SEVERITY_DROP, severity_detail=severity_detail)

# ---------------- network_degradation: scrape_duration_seconds > 1.5s ----------------
net_reps = [
    ('campaign1','2026-07-11T14:01:42Z','2026-07-11T14:03:20Z'),
    ('campaign2','2026-07-11T14:04:44Z','2026-07-11T14:06:22Z'),
    ('campaign3','2026-07-11T14:07:55Z','2026-07-11T14:09:31Z'),
    ('campaign4','2026-07-11T14:10:25Z','2026-07-11T14:12:02Z'),
    ('campaign5','2026-07-11T14:13:00Z','2026-07-11T14:14:37Z'),
    ('campaign6','2026-07-11T14:15:54Z','2026-07-11T14:17:33Z'),
    ('campaign7','2026-07-11T14:18:37Z','2026-07-11T14:20:14Z'),
    ('campaign8','2026-07-11T14:21:30Z','2026-07-11T14:23:07Z'),
]
THRESH_SCRAPE = 1.5
# Severity-target: 90% of the mean REAL per-rep maximum scrape_duration_seconds across all
# 8 reps' full actual Prometheus time series (4.280559766875s mean -> 3.8525037901875s) --
# note this uses the true scraped max, not the fault script's own self-reported
# peak_scrape_duration_seconds_during_fault field, which was found to understate the real
# peak in 3/8 reps (script samples 3x right after detection, misses later real scrapes).
SEVERITY_SCRAPE = 3.8525037901875
tp, fn, leads = 0, 0, []
severity_detail = []
for rid, inj, rec in net_reps:
    series = q('scrape_duration_seconds{job="kafka-broker-jmx"}', inj, rec, '5s')
    vals = [(int(t), float(v)) for t, v in series[0]['values']] if series else []
    crossing = next((t for t, v in vals if v > THRESH_SCRAPE), None)
    severity_ts = next((t for t, v in vals if v > SEVERITY_SCRAPE), None)
    if crossing:
        tp += 1
        leads.append(iso(rec).timestamp() - crossing)
    else:
        fn += 1
    severity_detail.append(dict(
        run_id=rid, crossing_utc=(iso('1970-01-01T00:00:00Z')+timedelta(seconds=crossing)).isoformat() if crossing else None,
        severity_utc=(iso('1970-01-01T00:00:00Z')+timedelta(seconds=severity_ts)).isoformat() if severity_ts else None,
        natural_end_utc=rec,
        crossing_to_severity_lead_s=(severity_ts - crossing) if (crossing and severity_ts) else None,
        severity_reached=severity_ts is not None,
    ))
gaps = [(net_reps[i][2], net_reps[i+1][1]) for i in range(len(net_reps)-1)]
fp_windows, checked, too_short = 0, 0, 0
for g_start, g_end in gaps:
    r = check_gap_fpr('scrape_duration_seconds{job="kafka-broker-jmx"}', g_start, g_end,
                       lambda series: any(float(v) > THRESH_SCRAPE for s in series for _, v in s['values']))
    if r is None:
        too_short += 1
    else:
        checked += 1
        fp_windows += int(r)
# Same SUPERSEDED situation as disk_pressure above -- see that comment.
results['network_degradation'] = dict(tp=tp, fn=fn, fp=fp_windows, gaps_checked=checked, gaps_too_short=too_short, leads_crossing_to_natural_end_s_SUPERSEDED=leads, severity_threshold_s=SEVERITY_SCRAPE, severity_detail=severity_detail)

pf.terminate()

print(json.dumps(results, indent=2, default=str))
