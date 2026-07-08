# KSPFail Project Rules

## Scope
docs/research_context.md is the locked scope. Objectives/methodology don't change mid-build,
except through the pivot rule below.

## Pivot rule
Time-box every blocker: one session (~2-3 hrs), not days.
- Tooling blocker -> fall back to documented fallback in research_context.md Section 11. Don't rebuild.
- Fundamental blocker (core hypothesis breaks) -> STOP. Flag it. Do not swap tools and push forward.

## Phase-gate rule (non-negotiable)
Before marking any phase complete:
1. Invoke the gate-auditor subagent against that phase's gate criteria.
2. Do not proceed to git push until gate-auditor returns PASS.
3. On FAIL: fix it, or explicitly log it in research_context.md as an accepted limitation.

## Git push rule
Never run `git push` unless the user's message explicitly says "push" or "push now."
Commit locally after every verified step regardless — commit discipline doesn't wait for network.

## Commit discipline
One commit per verified step. Commit message states what was verified, not just what was written.

## Python environment rule (non-negotiable)
Never run Python or install packages against system Python. Always use the project's `.venv`
(`.venv/bin/python`, `.venv/bin/pip`), gitignored, created at repo root. Applies to one-off
throwaway scripts too — no exceptions "just this once." Pin `requirements.txt` to versions
actually in use; add to it only when a package is actually needed, not speculatively.
