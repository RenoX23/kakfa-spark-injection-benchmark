# Anti-AI-Patterns: Writing With Real Specificity

The mechanism, stated once so it doesn't need repeating in every entry below: text
reads as generic model output when it's **vague, hedge-padded, and interchangeable**
with any other paper on a similar topic. The fix is never a synonym swap or a
detector-evasion trick — it's making every sentence say something that is specifically
and only true of *this* study. If a sentence would survive unchanged in a paper about a
completely different dataset or method, it's not doing its job yet.

Run this as a literal find-and-fix pass over a draft, not just a vibe check.

## Phrases to cut or replace on sight

These aren't banned because they're "AI phrases" in some superstitious sense — they're
generic filler that happens to be statistically overrepresented in unedited model
output, and they're generic filler in human writing too. Cutting them almost always
tightens the sentence.

| Pattern | Why it's empty | Fix |
|---|---|---|
| "delve into," "navigate the complexities of," "unpack" | Vague gesture at depth without adding any | Say what was actually examined |
| "it is important/worth noting that," "it should be noted that" | Throat-clearing — if it's worth noting, just note it | Delete the frame, keep the content |
| "plays a crucial/pivotal/vital role in" | Asserts importance instead of demonstrating it | State the actual mechanism or effect |
| "a testament to," "underscores the importance of" | Inflated significance-claiming without evidence | State what the result actually shows |
| "in today's fast-paced/rapidly evolving world/landscape" | Generic scene-setting that fits any topic | Cut entirely, or replace with the specific real trend/statistic driving the work |
| "boasts," "leverages," "harnesses" | Marketing-register verbs, not academic register | "uses," "employs," or name the specific mechanism |
| "not only X, but also Y" (used repeatedly) | A rhetorical crutch, fine once, a tell when it's the default sentence shape | Vary structure — most instances can just be "X and Y" |
| "moreover," "furthermore," "additionally," "in addition" stacked every paragraph | Mechanical transition-word padding | Most paragraphs don't need an explicit transition word at all if the logic already flows; use one only where the connection genuinely needs signposting |
| "a myriad of," "a plethora of," "numerous," "a variety of," "several studies have shown" | Vague quantification that dodges specifics | Give the actual number, or name the actual studies |
| "in conclusion," "to summarize," "overall" opening a Conclusion | Announces what the section already announces by existing | Just state the conclusion |
| "robust," "comprehensive," "significant" used as vague positive adjectives (not statistical "significant") | Empty praise-words that substitute for evidence | Replace with the specific metric/property that makes it true, or cut |
| Excessive hedging: "may potentially," "could possibly suggest," "it is possible that perhaps" | Double/triple-hedging reads as evasive, not careful | Pick one hedge word if uncertainty is genuinely warranted, state plainly if it isn't |

## Structural tells beyond word choice

- **Uniform sentence rhythm.** Model output tends toward similar-length sentences
  paragraph after paragraph. Real academic prose varies — a short, blunt sentence after
  two longer ones is a normal human pattern, not a mistake to smooth out.
  Deliberately vary length when revising.
- **Symmetric triads everywhere.** "This approach is efficient, scalable, and robust"
  — three-item lists in matched grammatical form, repeated as the default way to make
  any claim. Fine occasionally, a tell as a constant habit. Break the pattern: two items,
  four items, or a single specific claim instead of a list.
- **Every paragraph the same length and shape** (topic sentence, three supporting
  sentences, transition). Real writing has paragraphs that are two sentences and
  paragraphs that are eight, driven by how much the point actually needs, not a template.
- **Explaining the obvious before saying anything new.** A methodology paragraph that
  opens with a textbook definition of a well-known technique (e.g. defining what
  Random Forest is, in a paper whose readers are ML researchers) pads word count without
  adding information the actual audience needs.
- **Conclusions that restate the introduction almost verbatim** rather than actually
  synthesizing what was learned. If the closing paragraph could be produced by find-
  replacing a few words in the opening paragraph, it isn't doing its job.
- **Over-qualified claims that hedge away the actual finding.** "These results may
  potentially suggest a possible trend that could indicate..." — by the time a sentence
  hedges four times, it has said nothing falsifiable. State the actual finding, then
  state the actual limitation on it as a separate, specific sentence.

## The real fix, restated

Every instance above is fixed the same way: replace the generic phrase with the
specific fact, number, mechanism, or claim that is actually, uniquely true of this
study. "Our method demonstrates significant improvements" → "F1 improved from 0.667 to
0.909 after the N=8→N=15 top-up, though the shuffle-test p-value stayed at chance
(0.500 vs. 0.48)." The second version could not be pasted into any other paper. That's
the test.
