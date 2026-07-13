# Venue Notes: IEEE Access and the MDPI Journal Family

Grounded in each venue's current author instructions, checked directly (not recalled
from training data) on **2026-07-13**. Publisher policies change — if this file is more
than a few months old when you're reading it, re-verify anything load-bearing (word
limits, AI-disclosure wording, required back-matter sections) against the live pages
before relying on it for a submission. Sources are listed at the end of each section.

These are the two venue families this skill has real research behind. If the user's
target is Springer, Elsevier, ACM, or anything else, say explicitly that this file
doesn't cover it and either look it up fresh or fall back to the generic IMRAD +
numbered-citation defaults in `section-guide.md` / `citation-style.md`.

---

## IEEE Access

**Abstract.** Single **unstructured** paragraph — no subheadings, no numbered
equations, no numbered reference citations inside the abstract itself. No hard word
limit is published, but treat ~200 words as the practical ceiling; an abstract that
runs long usually means it's doing too much. Content should cover the problem, the
proposed approach, and the principal results/contributions — it has to work as a
standalone summary, since it's what shows up in IEEE Xplore and indexing services
independent of the full text.

**Keywords / Index Terms.** Two related but distinct things, don't conflate them:
- At submission, the portal asks for **3–10 manuscript keywords** used to route the
  paper to a relevant Associate Editor — free text, chosen for accuracy of match, not a
  controlled vocabulary.
- In the **published paper's Index Terms field**, IEEE's broader convention (Editage
  and general IEEE Author Center guidance) is to draw terms from the **IEEE Thesaurus**,
  typically 4–6 terms — don't invent index terms freestyle for that field.

**Author biographies.** Required for **every** author, placed after the references
section — a short paragraph each, standard IEEE format (academic background, current
role, research interests).

**Citation style.** Standard IEEE numbered bracket style — see `citation-style.md`.
IEEE Access specifically permits en-dash range citations like `[3]–[6]`, which some
other IEEE conference/transactions templates don't.

**AI-generated text disclosure — mandatory, specific mechanism.** If any text in the
article was AI-generated, it **must** be disclosed in the **Acknowledgments** section,
**and** the specific sections/passages that used AI-generated text must carry a
citation to the AI system used to generate them (i.e., a normal in-text citation
pointing at an acknowledgments-listed tool, not just a blanket disclaimer). This is
narrower than "you used an AI tool at some point in the workflow" — it's about
AI-*generated text* landing in the manuscript. AI used for editing/grammar assistance
on human-written text is generally treated as outside this disclosure requirement,
consistent with how academic publishers generally draw this line, but confirm current
wording before relying on that distinction for a specific submission.

**Formatting mechanics.**
- Official IEEE Access Word or LaTeX template — use it, don't approximate it by hand.
- Submit both the source file (Word/LaTeX) and a PDF that matches it exactly.
- Grammar review is a stated requirement before submission, not just good practice.
- Every abbreviation must be defined at first use — no exceptions for "well-known" ones.
- Practical page-count ceiling: under 20 pages; longer needs Editor-in-Chief approval.

Sources: [IEEE Access Submission Guidelines for Authors](https://ieeeaccess.ieee.org/authors/submission-guidelines/), [Structure Your Article — IEEE Author Center Journals](https://journals.ieeeauthorcenter.ieee.org/create-your-ieee-journal-article/create-the-text-of-your-article/structure-your-article/).

---

## MDPI (Electronics, Applied Sciences, and the wider MDPI journal family)

**Structure varies by journal — check the specific target before assuming.** The
default MDPI research-article shape is IMRAD: Introduction, Materials and Methods
(sometimes just "Methods"), Results, Discussion, and an optional Conclusions section —
but MDPI explicitly allows variation: some journals put Methods after Results/
Discussion, some merge Results and Discussion into one section, Conclusions is
mandatory in some journals and optional in others. Don't assume Electronics and Applied
Sciences use identical structure without checking each journal's own instructions page
at submission time — this file gives you the *default*, not a guarantee for one
specific journal.

**Abstract.** MDPI's general guidance is a structured *flow* without literal headings —
Background (context + purpose) → Methods (what was done) → Results (what was found) →
Conclusion (what it means) — narrated as one continuous paragraph, not four labeled
subsections. ~200 words is the general MDPI ceiling. **This is not universal across
every MDPI journal**: some (e.g., Journal of Clinical Medicine) require a literally
headed structured abstract (Background/Objectives, Methods, Results, Conclusions) at
~250 words. Confirm the exact format for the specific target journal before finalizing.

**Keywords.** Listed directly after the abstract; general MDPI convention is a small
set (commonly cited as roughly 3–10) of specific, non-generic terms — avoid repeating
words already prominent in the title, since keywords exist to widen discoverability,
not restate it.

**Citation style.** Numbered, square brackets, MDPI's own ACS-derived numbering
convention — see `citation-style.md` for the mechanics and worked reference-list
examples. MDPI also offers Chicago and APA style guides for journals that use those
instead of numbered citation — confirm which the target journal uses; numbered is the
default across most of the natural-science/engineering MDPI titles including Electronics
and Applied Sciences.

**Back matter — required sections, in this order** (skip any that don't apply, e.g. no
Institutional Review Board Statement for a systems/engineering paper with no human/
animal subjects, but say explicitly that it's "Not applicable" rather than omitting the
heading silently):

1. Supplementary Materials (if any)
2. Author Contributions
3. Funding
4. Institutional Review Board Statement
5. Informed Consent Statement
6. Data Availability Statement
7. Acknowledgments
8. Conflicts of Interest
9. Glossary/Nomenclature/Abbreviations (if needed)
10. Appendix (if needed)
11. References

**AI-generated content and authorship policy.**
- Generative AI tools/LLMs **cannot be listed as authors** — they don't meet MDPI's
  authorship criteria (accountability, ability to approve the final version).
- If a GenAI tool was used to **generate content** (not just edit it), authors must (a)
  disclose this in the **Acknowledgments** section, and (b) give a **detailed
  explanation of how the tool was used in the Materials & Methods section** — this is
  stricter than IEEE Access's single-location disclosure; MDPI wants it in two places.
- **Exception**: basic language-editing tasks (grammar, punctuation, structure,
  formatting correction on already-human-written text) do **not** require disclosure.
- Authors remain fully accountable for all content regardless of AI involvement in
  drafting it — this doesn't shift responsibility for accuracy, citations, or
  originality.

Sources: [MDPI Layout Style Guide](https://www.mdpi.com/authors/layout), [MDPI Reference List and Citations Style Guide](https://www.mdpi.com/authors/references), [MDPI's Updated Guidelines on Artificial Intelligence and Authorship](https://www.mdpi.com/news/5687), [How to Write an Abstract That Gets Your Paper Read — MDPI Blog](https://blog.mdpi.com/2026/06/04/writing-an-abstract/), [Back Matter in Research Articles: A Guide for Authors — MDPI Blog](https://blog.mdpi.com/2026/03/17/research-integrity-back-matter/).

---

## What this file deliberately does not cover

- Springer, Elsevier, ACM, and other publisher families — no research behind this file
  for them. Don't extrapolate IEEE/MDPI conventions onto them and present it as
  verified; look it up fresh or say plainly "unverified, treat as a guess."
- Journal-specific quirks below the family level (e.g., JCM's headed-abstract exception
  noted above exists precisely because MDPI is not uniform journal-to-journal). Always
  cross-check the *specific* target journal's current "Instructions for Authors" page
  before final formatting — this file gets you a correct, current *default*, not a
  substitute for that check.
