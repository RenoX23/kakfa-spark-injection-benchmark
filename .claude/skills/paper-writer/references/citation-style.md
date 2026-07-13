# Citation Style: IEEE and MDPI Mechanics

Both venues in `venue-notes.md` use **numbered, square-bracket in-text citations** with
a numbered reference list ordered by first appearance in the text — not
author-date/Harvard style. The two differ in reference-list entry formatting. This file
covers both plus the citation-hygiene rules that apply regardless of venue.

Verified against current guides on **2026-07-13** — sources at the end.

## In-text citation — shared mechanics

- Numbers in square brackets: `[1]`, `[1,2]`, `[2–4]` for a consecutive run of three or
  more (en dash, no spaces).
- Cite in the order references are first mentioned in the text — reference **1** is
  whichever source you cite first in the body, not alphabetical, not by importance.
- The bracket goes **before** terminal punctuation in a sentence: `...as shown in prior
  work [4].` not `...as shown in prior work. [4]`
- **Never use the reference number as a grammatical subject.** Wrong: `[4] proposed a
  new architecture.` Right: `Smith et al. [4] proposed a new architecture.` or `Ref. [4]
  proposed...` if you don't want to name the author inline.
- Multiple non-consecutive citations: comma-separated, no space — `[2,5,9]`, not
  `[2, 5, 9]`.
- IEEE Access specifically allows en-dash ranges (`[3]–[6]`); confirm before assuming
  every IEEE venue does — some conference templates don't.

## Reference list — IEEE format

Numbered flush-left in brackets, one entry per number, in citation order. First six
authors listed individually; use "et al." for a seventh or beyond. Author names as
initials + surname, not full first names.

**Journal article:**
```
[1] M. Long and K. Taxman, "Induction of electric field in fat eagles nesting in
    coconut trees," IEEE Trans. Biomed. Eng., vol. 28, pp. 287–299, Oct. 2015,
    doi: 10.1109/TBME.2001.2658415.
```

**Book:**
```
[2] K. Matzuki, Y. Watanabe, and H. Takashima, Japan's Railway System, Tokyo:
    Nihon Keizai Hyoronsha, 2017.
```

**Book chapter:**
```
[3] T. V. Pump and G. M. Chap, "Why we love to eat fish," in The Joy of Telling
    Stories, P. G. Peton, Ed., Princeton, NJ: Princeton Univ. Press, 1965, pp. 124–133.
```

**Conference paper:**
```
[4] C. Naly and S. Gogol, "Wavelength-switched passively coupled single-mode
    optical network," in Proc. IEEE Antennas Propag. Soc. Int. Symp., Jul. 2017,
    pp. 4322–4334.
```

Notes: journal/conference/book titles are italicized in the actual manuscript (shown
here as plain text — apply italics when writing into the real document). Every entry
ends with a period, except a trailing DOI/URL (period goes after the DOI). Give a DOI
whenever one exists — reviewers and typesetters both check for it.

## Reference list — MDPI format

Author surnames + initials, semicolon-separated between authors (not "and" before the
last one, unlike IEEE). Journal names abbreviated per **ISO 4** rules, not spelled out
in full or invented ad hoc.

**Journal article:**
```
1. Díaz, D.D.; Converso, A.; Sharpless, K.B.; Finn, M.G. 2,6-Dichloro-9-
   thiabicyclo[3.3.1]nonane: Multigram Display of Azide and Cyanide Components on a
   Versatile Scaffold. Molecules 2006, 11, 212–218.
```
Pattern: `Author1; Author2; ... Title of the article. Journal Abbrev. Year, Volume,
FirstPage–LastPage.`

**Book:**
```
2. Desiraju, G.R.; Steiner, T. The Weak Hydrogen Bond in Structural Chemistry and
   Biology, 2nd ed.; Oxford University Press: New York, NY, USA, 1999; pp. 10–25.
```
Pattern: `Author1; Author2; ... Title of the Book, Edition; Publisher: City, Country,
Year; pp. Range.`

**Conference paper:**
```
3. Chum, O.; Philbin, J.; Zisserman, A. Near duplicate image detection: Min-Hash and
   tf-idf weighting. In Proceedings of the 19th British Machine Vision Conference
   (BMVC 2008), Leeds, UK, 1–4 September 2008; pp. 812–815.
```
Pattern: `Author1; Author2; ... Title of Presentation. In Proceedings of the Name of
the Conference, Location, Country, Date (Day Month Year); pp. Range.`

MDPI also has separate Chicago- and APA-style guides for journals that don't use the
numbered style (mostly humanities/social-science MDPI titles) — the numbered format
above is the default for the natural-science/engineering titles this skill's venue
notes actually cover (Electronics, Applied Sciences). Confirm which the target journal
uses before applying this blindly.

## Citation hygiene — applies regardless of venue

These are the rules that matter more than either format above, because getting them
wrong is a research-integrity problem, not a typesetting one.

1. **Every citation must be a real, checkable work.** If you (the assistant) are not
   certain a citation exists and is correctly attributed — title, authors, venue, year
   all genuinely matching what's being claimed — do not write it in as if it's real.
   Insert `[CITATION NEEDED — verify: <what claim needs a source>]` and say so out loud
   to the user. This is `paper-writer`'s Non-negotiable Rule 1, restated here because
   it's the single most common way an AI-assisted draft becomes actively dangerous to
   submit.
2. **A citation must support the specific claim it's attached to**, not just be
   topically adjacent. "Prior work has explored fault injection in distributed systems
   [7]" needs `[7]` to actually be about fault injection in distributed systems, not a
   paper that mentions distributed systems in passing.
3. **Don't cite a source you haven't verified the actual content of.** Citing a title
   that sounds right without confirming what the paper actually says/found is how
   citation-chain errors propagate — one paper misdescribes an earlier one, the next
   paper cites the misdescription instead of the original, and it compounds.
4. **Match reference-list metadata exactly** — volume, issue, pages, DOI. A citation
   that's "close enough" fails automated reference-checking tools most venues now run,
   and looks like fabrication even when the underlying source is real but the metadata
   was guessed.
5. **When the user gives you a citation verbally** ("there's a 2023 paper by Kim on
   this"), treat it as a lead to verify, not a finished reference-list entry — ask for
   the DOI/link, or flag it as unverified until confirmed, don't silently invent the
   rest of the bibliographic details to make it look complete.

Sources: [IEEE Paper Formatting Guidelines and Template Explained — Editage Insights](https://www.editage.com/insights/ieee-citation-style-references-to-different-works-in-ieee-format), [MDPI Reference List and Citations Style Guide](https://www.mdpi.com/authors/references).
