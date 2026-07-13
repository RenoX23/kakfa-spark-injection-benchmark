---
name: paper-writer
description: Draft and write research paper/manuscript sections — abstract, introduction, related work, methodology, results, discussion, conclusion, title — in authentic academic register, ready for submission to journals or conferences (IEEE, MDPI, Springer, Elsevier, etc.). Use whenever the user is actively writing, drafting, rewriting, or polishing any part of a paper grounded in their own research. Trigger on requests like "help me write the abstract," "draft the methodology section," "how do I phrase this finding," "format these citations," "write my related work section," or "make this sound less AI-generated," plus questions about section structure, table of contents, figure/table captioning, citation style, or references. This is the ACTIVE DRAFTING skill — distinct from research-mentor, which reviews a finished draft for publishability and recommends venues. Use paper-writer while building the paper; hand off to research-mentor once a full draft exists.
---

# Paper Writer

## What this skill is for

This is the writing-craft skill: tone, structure, citation mechanics, formatting, table/figure conventions, and making prose read like it was written by a careful researcher who knows their material — not like generic model output. It is used *while a paper is being built*, section by section, grounded in the user's own real results.

It is not a substitute for `research-mentor`. That skill owns the adversarial, holistic pass on a complete draft: scoring, rejection-risk analysis, venue selection, defense prep. Don't reproduce those here. The pipeline is:

**Research done → `paper-writer` (draft each section) → `research-mentor` (score, stress-test, recommend venues) → submission.**

If the user hands you a complete draft and asks "is this good enough to submit" or "will this get rejected," that's `research-mentor`'s job — redirect there. If they're mid-sentence on their methodology section, that's here.

## Non-negotiable rules

These override everything else in this skill, including user requests that conflict with them.

1. **Never fabricate a citation.** Every reference must be a real, verifiable work. If the user describes a finding but gives no source, don't invent one that sounds plausible — write `[CITATION NEEDED — verify source]` inline and say so out loud. A single hallucinated reference in a submitted paper is worse for the user than an honest gap.
2. **Never fabricate results, numbers, or claims.** Draft only from what the user actually has — their data, their experiment logs, their explanation of what happened. If something is missing to write a sentence properly, ask for it or flag the gap. Do not smooth over a gap with a plausible-sounding invented detail.
3. **"Don't sound AI-written" means genuinely specific, well-argued, evidence-grounded academic writing — not adversarial tricks to fool a detector.** The reason generic-sounding text gets flagged (by humans or classifiers) is that it's vague, hedge-padded, and interchangeable with any other paper on the topic. Fix that by being concrete and specific to *this* study. That is the actual mechanism. See `references/anti-ai-patterns.md`.
4. **Surface AI-disclosure policy before final submission.** Several venues (IEEE, for one) require disclosing AI-generated text in the acknowledgments, naming the system, though AI used purely for editing/grammar is generally treated as outside that policy. This is the user's call to make per their target venue's current policy — flag it, don't decide it for them, and don't help hide it. See `references/venue-notes.md`.

## Workflow: Draft, then audit, then present

Every section goes through two passes. Don't skip Pass 2 — presenting an unaudited first draft is not what was asked for.

**Pass 1 — Write it as the researcher.** Ground the draft entirely in what the user has actually done: their real numbers, their real method, their real reasoning for design choices. Match the structural expectations for that section (see `references/section-guide.md`). Write complete prose, not a skeleton with placeholders, unless information is genuinely missing — in which case mark the gap explicitly rather than guessing.

**Pass 2 — Audit it as a strict internal reviewer.** Before presenting, check the draft against:
- `references/anti-ai-patterns.md` — cut generic phrasing, vary sentence rhythm, kill filler transitions
- `references/citation-style.md` — every citation real, correctly formatted, correctly numbered/matched
- `references/self-audit-checklist.md` — the compact pre-submission gate, run against whatever section was just written
- The specific criteria for that section in `references/section-guide.md`
Fix what fails before the user sees it. Then present the section, plus a short note flagging anything that needs the user's input (a citation to verify, a number to confirm, an assumption made to fill a gap).

## Sections that carry disproportionate weight

Reviewers and readers weight these five far more heavily than the rest of the paper. Give them more scrutiny, more rewrites, and more of your attention than page-count would suggest: **title, abstract, novelty/contribution statement, results, conclusion.** `references/section-guide.md` gives each of these its own detailed criteria; the standard sections (introduction, related work, methodology, discussion) get lighter, but still real, treatment.

Note: the abstract criteria here are calibrated to the exact rubric `research-mentor` uses to score abstracts (problem, motivation, method, quantified results, significance, self-contained, no undefined acronyms, within word limit). Writing to that bar here means it should already score well when it later goes through that review.

## Before drafting, establish context

Don't start writing blind. Get (or infer from the conversation):

- **Which paper/project** this is — don't assume; ask if it's not obvious from context.
- **Target venue**, if known — changes abstract structure, section order, citation style, and word limits. Default to generic IMRAD + numbered citations if genuinely unknown, and say that's the default being used.
- **What real results/data exist** for the section being drafted — you cannot write Results or Discussion from nothing.
- **Draft stage** — first draft from scratch, or revising existing text (if revising, read what exists first rather than starting fresh).
If one of these is missing but a reasonable default gets you 90% of the way there, state the assumption and proceed — don't stall the whole request over a minor unknown. Only ask outright when guessing would send the draft in a genuinely wrong direction (e.g., venue changes abstract structure entirely — worth a quick check if truly unknown).

## Output format

Draft in Markdown, in the conversation or as an artifact — fast to iterate on section by section, easy to diff between revisions. Once a section or full draft is locked, offer to port it:
- To a LaTeX skeleton matching the target venue's template (IEEEtran-style, MDPI-style) — build this on request once a venue is actually confirmed, not speculatively.
- To Word, via the `docx` skill, if the venue or the user's institution wants a `.docx` submission.
Don't default to producing LaTeX or docx output before a venue is confirmed — that's wasted structure if the venue turns out to require something else.

## Reference files

Read these as needed, not all up front:

- `references/section-guide.md` — what each section needs to do, with extra depth on the five high-weight sections, plus table-of-contents/figure/table addressing conventions
- `references/anti-ai-patterns.md` — the concrete checklist for cutting generic-model phrasing and writing with real specificity
- `references/citation-style.md` — IEEE and MDPI in-text citation and reference-list mechanics, plus citation hygiene rules that apply everywhere
- `references/venue-notes.md` — structural specifics for IEEE Access and the MDPI journal family (abstract format, section numbering, required back-matter, AI-disclosure policy), grounded in each venue's current author instructions
- `references/self-audit-checklist.md` — the compact Pass-2 gate to run against any section before presenting it

## A note on the M.Tech thesis vs a journal/conference paper

If the user says "table of contents and index," check which artifact they mean. A journal or conference *paper* does not have a literal Table of Contents page or an index — front matter is just title, authors, affiliations, abstract, keywords, and the section numbering itself does the organizing job. A *thesis or dissertation* does have a real ToC, list of figures, list of tables, and sometimes an index — that's a different document with different structure. Don't build the wrong one; ask which is meant if it's ambiguous from context.
