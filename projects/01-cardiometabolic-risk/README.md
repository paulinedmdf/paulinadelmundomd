# 01 — Cardiometabolic Risk in NHANES 2015–2018

[![Quarto](https://img.shields.io/badge/built%20with-Quarto-2C7BB6?logo=quarto)](https://quarto.org/)
[![R 4.5.2](https://img.shields.io/badge/R-4.5.2-276DC3?logo=r)](https://www.r-project.org/)
[![reproducible: renv](https://img.shields.io/badge/reproducible-renv-2C7B6E)](../../renv.lock)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](../../LICENSE)

A polyglot R + Python analysis of metabolic-syndrome prevalence and risk in US adults — pooled NHANES cycles, multiple imputation, design-aware logistic regression, gradient boosting with SHAP, and a Pooled Cohort Equations head-to-head comparator.

**Live notebook:** [analysis.html](analysis.html)
**Source:** [analysis.qmd](analysis.qmd)
**Helpers:** [`R/nhanes_helpers.R`](../../R/nhanes_helpers.R)

---

## Headline findings

- **Survey-weighted prevalence ≈ 33%** in US adults ≥ 20 years (NHANES 2015–2018, MI-pooled). The unweighted complete-case figure is ~5 points lower — that gap is what design weights and MI are built to recover.
- **Definition matters by ~5 percentage points.** NCEP ATP III, IDF 2005, and JIS 2009 produce a defensible range; JIS shifts upward in Asian and Mexican-American subpopulations because of lower waist cutoffs.
- **PCE inputs beat demographics + BMI on the same MetS outcome — and that's the *expected* result.** PCE inputs include 3 of the 5 MetS components, so the comparison is "what fraction of the signal sits in cardiovascular labs vs pure adiposity," not a clean head-to-head.
- **Calibration > AUC for clinical portability.** Reliability curve hugs the diagonal across deciles; AUC sits in 0.78–0.83 for the boosted model.

## Methods

| Layer | Approach |
|---|---|
| Data | NHANES 2015–2016 (cycle I) + 2017–2018 (cycle J), pooled per CDC analytic guidelines (`WTMEC4YR = WTMEC2YR / 2`) |
| Outcome | Metabolic syndrome (NCEP ATP III primary; IDF 2005 + JIS 2009 sensitivity) |
| Missingness | Multiple Imputation by Chained Equations (`mice`, m=5, predictive mean matching) with Rubin's-rules pooling |
| Inference | Design-aware logistic regression (`survey::svyglm`) with Taylor linearization SEs |
| ML | Gradient-boosted classifier (XGBoost, depth 4, lr 0.05) with SHAP interpretability |
| Comparator | Pooled Cohort Equations 10-year ASCVD risk (Goff et al. 2014) — both construct-validity check and same-outcome AUC head-to-head |
| Reporting | TRIPOD+AI checklist as Appendix |

## Reproducing locally

```r
# 1. Clone and set working directory
setwd("paulinadelmundomd")

# 2. Restore the exact R package versions used
renv::restore()

# 3. Render
quarto::quarto_render("projects/01-cardiometabolic-risk/analysis.qmd")
```

## Caveats

- Cross-sectional, not incident — a true PCE head-to-head needs the NHANES Linked Mortality Files (NHANES-LMF) and a survival model
- No external validation cohort — 25% holdout is internal, not external
- PCE itself only supports non-Hispanic White / non-Hispanic Black race assignments; everyone else gets NA in the PCE comparator (a limitation of the score, not this implementation)

## Status

Pre-publication portfolio analysis — no peer review, no journal submission yet.

## Citation (interim)

> Del Mundo Del Fierro, P. (2026). *Cardiometabolic Risk in NHANES 2015–2018: a polyglot R + Python analysis.* Personal portfolio. https://paulinadelmundomd.com/projects/01-cardiometabolic-risk/analysis.html
