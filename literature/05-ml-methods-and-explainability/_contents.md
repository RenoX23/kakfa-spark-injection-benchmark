# 05 — ML Methods & Explainability (methods actually used)

The models and interpretability method KSPFail runs. All stock, canonical implementations — no novel
algorithm is claimed (see `docs/research_context.md` §7). Cite where the methodology names each.

| File | Paper | Year | ID |
|------|-------|------|----|
| `breiman2001-random-forests.pdf` | Breiman — *Random Forests* | 2001 | DOI 10.1023/A:1010933404324 |
| `chen2016-xgboost.pdf` | Chen, Guestrin — *XGBoost: A Scalable Tree Boosting System* | 2016 | DOI 10.1145/2939672.2939785 |
| `ke2017-lightgbm.pdf` | Ke et al. — *LightGBM: A Highly Efficient Gradient Boosting Decision Tree* | 2017 | NeurIPS 2017 |
| `lundberg2017-shap.pdf` | Lundberg, Lee — *SHAP: A Unified Approach to Interpreting Model Predictions* | 2017 | arXiv:1705.07874 |

RF / XGBoost / LightGBM are the three classifiers benchmarked; **SHAP** is the interpretability method used
for the mechanism-verification discipline (e.g. the disk_pressure lead-time cross-check). PDF `/Title` and
`/Author` metadata were confirmed to match for the LightGBM proceedings copy. Full detail in
[`../README.md`](../README.md).
