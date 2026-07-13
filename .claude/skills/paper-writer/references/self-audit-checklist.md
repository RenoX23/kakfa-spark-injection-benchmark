# Self-Audit Checklist (Pass 2)

Run this against any section before presenting it to the user — it's the compact gate
referenced in `SKILL.md`'s workflow. Each item should be a genuine check, not a
rubber stamp; if something fails, fix it before presenting, don't note it as a caveat
and ship it anyway unless the fix genuinely requires information only the user has.

## Integrity (check first — these are non-negotiable, not style preferences)

- [ ] Every citation is a real, verifiable work — no invented author/title/venue
      combination, even a plausible-sounding one. Unverifiable claims are marked
      `[CITATION NEEDED — verify: ...]`, not silently sourced.
- [ ] Every number, result, and quantitative claim traces to something the user
      actually provided (data, logs, a stated result) — nothing estimated or
      invented to complete a sentence.
- [ ] No claim is stronger than what the underlying evidence supports. A mixed or
      negative result is stated as such, not softened into an implied positive one.
- [ ] Any gap (missing citation, missing number, an assumption made to fill a hole)
      is flagged explicitly to the user, not smoothed over silently.

## Structure

- [ ] The section does what `section-guide.md` says that section needs to do — check
      the specific criteria for that section, not just "does it look like prose."
- [ ] If this is one of the five high-weight sections (title, abstract, novelty
      statement, results, conclusion), it was held to that section's detailed
      criteria, not just the lighter standard-section pass.
- [ ] Figures/tables (if any) are captioned per `section-guide.md`'s Table/Figure
      conventions, numbered correctly, and referenced by number in the body text.
- [ ] Section fits the target venue's structural expectations from `venue-notes.md`
      (or states plainly that venue is unknown/unverified and generic IMRAD defaults
      were used instead).

## Citation mechanics

- [ ] In-text citation format matches the venue (`citation-style.md`) — numbered
      brackets, correct ordering, no reference number used as a sentence subject.
- [ ] Reference-list entries (if being compiled) match the venue's exact format —
      author list style, title case, journal abbreviation, page range format.
- [ ] Citations are cited in ascending order when combined (`[2,5,9]` not `[9,2,5]`).

## Prose quality

- [ ] Ran the `anti-ai-patterns.md` phrase table against the draft — generic filler
      phrases replaced with specific, study-grounded claims.
- [ ] Sentence rhythm varies — not uniform-length sentences or mechanically
      symmetric triads throughout.
- [ ] No paragraph could be pasted unchanged into a different paper on a similar
      topic without anyone noticing.
- [ ] Every abbreviation is defined at first use (per-section, or per-paper if the
      venue expects that — check `venue-notes.md`).
- [ ] Conclusion (if this is the Conclusion) introduces no result or claim that
      wasn't already established in Results/Discussion.

## Before presenting to the user

- [ ] A short note is ready flagging: any citation that needs verification, any
      number that needs the user's confirmation, any assumption made to fill a
      genuine information gap, and (if this is a late-stage draft) whether an
      AI-disclosure statement will be needed per the target venue's policy
      (`venue-notes.md`).
- [ ] If venue is confirmed and this is a near-final section, note whether it's
      ready to port to LaTeX/docx — but don't produce that output unprompted per
      `SKILL.md`'s Output Format guidance.
