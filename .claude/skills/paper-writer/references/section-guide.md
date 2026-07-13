# Section Guide

What each section needs to accomplish, not just what to put in it. The five sections
listed in the main `SKILL.md` as disproportionately weighted get their own detailed
subsection with explicit pass/fail criteria; the rest get lighter, but still concrete,
guidance. Structural conventions (IMRAD, back matter) are venue-dependent — see
`venue-notes.md` for IEEE Access and MDPI specifics; this file is about what makes each
section *good*, which holds across venues.

---

## High-weight sections (extra scrutiny)

### Title

A reviewer or reader decides whether to open the paper based on the title alone more
often than any other single element. Criteria:

- **States the actual contribution, not just the topic area.** "A Study of Failure
  Prediction in Kafka/Spark Pipelines" is a topic. "Fault-Injection Benchmarking Reveals
  Limited Lead-Time Advantage of ML over Static Thresholds in Streaming Pipelines" is a
  contribution — a reader learns the finding, not just the field.
- **No undefined abbreviations.** If the full paper is about "KSPFail," the title
  doesn't get to assume the reader knows what that is.
- **Specific, not generic-sounding.** If you could swap in a different dataset/method
  and the title would still basically work, it's too generic. Anchor it to what makes
  *this* paper different — the method, the domain, the finding.
- **Length**: most venues expect roughly 10–15 words; longer titles usually mean two
  ideas fighting for one sentence — split the difference or pick the more important one
  and demote the rest to a subtitle after a colon.
- Avoid "Towards," "A Study of," "An Investigation into," "Exploring..." as opening
  words — they're filler that every third paper title also uses, and they signal
  hedging before the reader has read a single result.

### Abstract

Calibrated to the exact rubric `research-mentor` scores against, so writing to this bar
here means it should already pass that review. Every abstract needs all of the
following, in roughly this order, as continuous prose (not headed subsections, per
`venue-notes.md`, unless the specific target journal requires literal headings):

1. **Problem** — what gap or limitation motivates this work, stated concretely (not
   "many challenges remain in X").
2. **Motivation** — why this problem matters, in one sentence, not a paragraph.
3. **Method** — what was actually done. Name the real approach (e.g. "LOO-CV
   evaluation of a Random Forest classifier on fault-injection telemetry from a Kind
   cluster") not a vague gesture ("we propose a machine learning framework").
4. **Quantified results** — actual numbers from the actual study. "Our method achieves
   strong performance" is not a result; "F1=0.941 (p<0.01) for disk_pressure, with no
   class clearing chance-level significance for the other four fault types" is.
5. **Significance** — what the result means for the field or for practice, stated
   honestly relative to what was actually shown (don't inflate a negative or mixed
   result into an implied positive one).
6. **Self-contained** — a reader who has not read the rest of the paper can understand
   the abstract on its own. No "as shown below," no forward references to figures.
7. **No undefined acronyms.** Every abbreviation used in the abstract is either spelled
   out on first use inside the abstract itself, or common enough to not need it (e.g.
   "ML," "API") — judge this per-venue, not by assumption.
8. **Within the venue's word limit** — see `venue-notes.md`. If unknown, target 200
   words as a safe default and say that's what you're targeting.

A negative or mixed result belongs in the abstract with the same confidence as a
positive one — burying it, or writing around it with vague language, is both an
academic-integrity problem and something an alert reviewer will catch instantly and
penalize harder than the negative result itself would have.

### Novelty / Contribution statement

Usually the closing paragraphs of the Introduction, sometimes a bulleted list. This is
where papers most often oversell. Criteria:

- **Specific and falsifiable**, not "we present a novel framework." State exactly what
  is new: a method, a dataset, a benchmark, an empirical finding, a negative result that
  corrects a prior assumption in the field.
- **Distinguished from related work explicitly**, not just implicitly by omission — if
  the contribution is "the first fault-injection benchmark for Kafka+Spark pipeline
  failure prediction," that claim needs the Related Work section to have actually
  established that no prior benchmark does this, not just gone unmentioned.
- **Scoped honestly.** If the contribution is a benchmark + methodology rather than a
  state-of-the-art model, say that — don't let contribution-statement language imply a
  performance claim the Results section doesn't back up.
- **Each bullet earns its place.** If a "contribution" is actually a implementation
  detail or a restatement of the method, cut it — a padded contribution list reads as
  weaker than a tight one, not stronger.

### Results

- **Report what was found, not what you hoped to find.** A negative or null result,
  reported with the same rigor as a positive one, is a legitimate contribution — burying
  it or hedging around it is worse than the negative result itself.
- **Every number needs a source** — a table, a figure, or an explicit computation
  described in Methods. If you (the assistant) don't have the actual number from the
  user, don't estimate one to fill the sentence; ask or flag the gap.
- **Statistical claims need the actual test, not just the point estimate.** "F1=0.941"
  alone is a number; "F1=0.941, 0/100 shuffled labels scored ≥ real (p<0.01)" is
  evidence. If the user's own work has a significance test, cite its actual result — not
  a plausible-sounding one.
- **Tables and figures carry real information density**, not decoration. Every
  table/figure needs a caption that lets it be understood without re-reading the body
  text (see Table/Figure conventions below) and needs to actually be referenced by
  number in the prose near where it's discussed.
- **Separate what the data shows from what it means.** Interpretation belongs in
  Discussion (or a combined Results-and-Discussion section, if the venue allows it) —
  Results states facts, doesn't yet argue about their implications.

### Conclusion

- **Restates the actual contribution and actual findings**, not a re-hash of the
  Introduction's motivation paragraph. If a reader only reads the Abstract and the
  Conclusion, they should get the same core claims from both, not a narrower/vaguer
  version in the Conclusion.
- **States limitations honestly and specifically** — "small sample size for class X,"
  "single-cluster deployment, generalization to production scale untested" — not a
  generic "further work is needed" that could apply to any paper in the field.
- **Future work is concrete**, tied to a specific open question the paper itself
  surfaced, not a generic wishlist ("more data," "deep learning," "other datasets")
  disconnected from what was actually learned.
- **No new results or claims introduced here for the first time.** If a number or
  finding appears in the Conclusion, it needed to already be in Results/Discussion.

---

## Standard sections (real treatment, less scrutiny than the five above)

### Introduction

Funnel structure: broad context → specific problem → gap in existing approaches → what
this paper does about it → (often) a short paragraph previewing the paper's structure.
Common failure mode to watch for: spending too many paragraphs on broad motivation
before naming the actual gap — a reader should know what problem this paper solves by
the end of paragraph 2 or 3, not paragraph 6.

### Related Work

Organize by theme or approach, not as a flat list of "Author X did Y. Author Y did Z."
Every related-work paragraph should end by positioning the current paper relative to
what it just described — what's shared, what's different, why the difference matters.
A related-work section that never says "unlike [12], this work does X" for any of its
citations is doing description, not argument, and won't support a strong novelty claim
later.

### Methodology

Needs enough detail that a competent reader in the field could reproduce the study
without contacting the authors. Concrete requirements:
- Every design decision that isn't obvious gets a one-sentence justification (why this
  model, why this train/test split, why this threshold) — grounded in the user's actual
  reasoning, not an invented-sounding rationale.
- State what was tried and rejected, if that's part of the real story (methodology
  sections that only show the final choice, with no acknowledgment of alternatives
  considered, often read as less credible, not more).
- Split cleanly from Results: Methodology describes what was done and how it will be
  evaluated; it does not report outcomes.

### Discussion

- Interprets the Results, doesn't repeat them. If a Discussion paragraph could be
  produced by rephrasing a Results sentence without adding any new interpretive claim,
  cut it.
- Explicitly engages with results that *don't* fit the expected story — a Discussion
  that only explains the results that support the hypothesis, and is silent on ones that
  don't, is the single most common tell of a weak or dishonest paper.
- Compares against related work by name where relevant, not just in the abstract
  Related Work section — "this result is consistent with/contradicts [9]'s finding
  that..." is what makes a Discussion section actually discuss something.

---

## Table of Contents, Figures, and Tables

- **A journal/conference paper does not get a literal Table of Contents page.** Section
  numbering (1, 1.1, 2, 2.1...) does that organizing job. See the note in the main
  `SKILL.md` about not confusing a paper with a thesis, which does have a real ToC.
- **Figure and table numbering is sequential and separate**: Figure 1, Figure 2... and
  Table 1, Table 2... as two independent sequences, not interleaved into one.
- **Captions go below figures, above tables** — this is the near-universal convention
  across IEEE and MDPI; don't reverse it.
- **A caption is a self-contained summary**, not just a label. "Figure 3. SHAP
  attribution for the disk_pressure delta-feature classifier, showing magnitude features
  (delta_mean/min/max/last) carrying 97.4% of total attribution across 16 true-positive
  windows." is a real caption. "Figure 3. SHAP results." is not.
- **Every figure/table is referenced by number in the body text**, near where it's
  discussed — never a figure that just sits there unreferenced, and never a forward
  reference so far ahead of the actual discussion that the reader loses the connection.
- **Axis labels, legends, and units are non-negotiable** on any figure going into a
  submission — an unlabeled axis is an automatic revision request at most venues.
