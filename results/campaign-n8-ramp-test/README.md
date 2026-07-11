This directory held calibration/diagnostic reps for the executor_oom gradual-ramp
redesign, not the final dataset. Its final 8 reps (ramptest3 through ramptest10) have
since been moved into results/campaign-n8/executor_oom/ as the active dataset; the one
rep among the calibration set with a real config-bug artifact (ramptest2, recovered:
false) moved to results/campaign-n8/_discarded/executor_oom-old-maxfailures/ with its own
explanation there.

What's left here is executor_oom_kill_runramptest1.json: the first ramp-design attempt,
100MB chunks / 16 chunks / 9s sleep, undershot the 180-240s target at 61s with only a
single-step signal (didn't achieve the intended multi-point rise). Superseded by the
recalibrated design (25MB chunks / 36 chunks / 7.5s sleep, based on measured baseline
~484MB and ~724MB gap to the 1152Mi limit) used for ramptest2 onward. Kept as evidence of
the calibration process, not part of any active or discarded dataset claim.
