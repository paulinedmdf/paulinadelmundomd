# 04 — The $35 Insulin Cap and Medicare Part D

[![Quarto](https://img.shields.io/badge/built%20with-Quarto-2C7BB6?logo=quarto)](https://quarto.org/)
[![R 4.5.2](https://img.shields.io/badge/R-4.5.2-276DC3?logo=r)](https://www.r-project.org/)
[![DuckDB](https://img.shields.io/badge/streaming-DuckDB-FFF000)](https://duckdb.org/)
[![reproducible: renv](https://img.shields.io/badge/reproducible-renv-2C7B6E)](../../renv.lock)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](../../LICENSE)

A difference-in-differences evaluation of the Inflation Reduction Act's $35/month Part D insulin out-of-pocket cap (effective 2023-01-01), using six annual releases of the CMS Medicare Part D Prescribers — by Provider and Drug public use file. Two-way fixed-effects DiD with non-insulin diabetes drugs as the control group, an event-study to test parallel pre-trends, and placebo / leave-one-out robustness.

**Live notebook:** [analysis.html](analysis.html)
**Source:** [analysis.qmd](analysis.qmd)
**Prep pipeline:** [prep.py](prep.py)

---

## The policy and why it has a clean DiD design

The IRA capped Medicare Part D beneficiary out-of-pocket cost for insulin at $35 per month-of-supply, effective 2023-01-01 — applied uniformly to every Part D plan (standalone PDPs and MA-PDs alike). The CMS Senior Savings Model (SSM) demonstration had already extended a similar cap to participating MA-PD plans starting 2021-01-01, so 2021 sits in the panel as a partially-treating shock that the event-study can absorb separately.

Because the cap applies to *insulin specifically*, all other Part D-covered antihyperglycemics are an honest control group. Treatment is simultaneous (every covered insulin product is treated on the same date), so a straightforward two-way fixed-effects estimator is unbiased — the staggered-adoption complications that motivate Callaway–Sant'Anna or Sun–Abraham don't apply here.

## Headline findings

- *Populated on first render against `data/processed/drug_year_panel.parquet`.* The notebook reports the ATT (post-2023 effect on insulin 30-day fills, relative to the non-insulin diabetes-drug counterfactual) with 95% CI clustered at the drug level, the event-study plot, and the joint pre-trends F-test.

## Methods

| Layer | Approach |
|---|---|
| Source | CMS Medicare Part D Prescribers — by Provider and Drug, annual files DY2019–DY2024 |
| Storage | DuckDB streaming filter over the per-year CSVs to a drug × year and prescriber × drug × year panel |
| Treatment definition | Generic name matches `INSULIN` (any insulin mono- or combination product) |
| Control definition | Curated list of non-insulin antihyperglycemic generics: metformin, sulfonylureas, TZDs, DPP-4i, GLP-1 RAs, SGLT2i |
| Primary outcome | Total 30-day fills (`Tot_30day_Fills`) at the drug-year level, log-transformed |
| Secondary | Total claims, total day-supply, total drug cost, beneficiary count |
| Identification | Two-way fixed effects (drug + year) with `treated × post-2023` interaction; SEs clustered at the drug level |
| Pre-trends | Event-study with year-by-treatment interactions; joint F-test on pre-period leads |
| Placebos | Fake cap years (2020, 2021), drug leave-one-out, alternate control subsets (drop GLP-1 RAs given concurrent demand shock) |
| Heterogeneity | Re-estimate at the prescriber-drug-year level with prescriber FE, split by prescriber specialty (endocrinology vs primary care vs other) |

## Reproducing locally

```bash
# 1. Download the per-year "Medicare Part D Prescribers - by Provider and Drug"
#    CSVs from CMS and place them under data/raw/ with their CMS filenames,
#    e.g. MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv. CMS uses 2-digit data years
#    (DY22 = 2022). Landing page: https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers

# 2. Run the DuckDB prep — emits the three committed Parquets in data/processed/
python projects/04-medicare-part-d-insulin-cap/prep.py

# 3. Restore the R package versions and render
Rscript -e 'renv::restore()'
quarto render projects/04-medicare-part-d-insulin-cap/analysis.qmd
```

The .qmd reads only the committed Parquets, so re-rendering doesn't require the raw CSVs once `prep.py` has been run once.

## Data sources

- **CMS Medicare Part D Prescribers — by Provider and Drug.** Centers for Medicare & Medicaid Services. <https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers>
- **IRA Section 11406** — caps cost-sharing for covered insulin products under Part D at the lesser of $35, 25% of the negotiated price, or the deductible. Effective 2023-01-01.
- **CMS Part D Senior Savings Model.** <https://www.cms.gov/priorities/innovation/innovation-models/part-d-senior-savings-model>

## Caveats

- **Drug cost is total, not beneficiary OOP.** The PUF reports `Tot_Drug_Cst` (Medicare + plan + beneficiary). The cap shifts cost from beneficiary to plan; it does not lower total spending per fill. Utilization-based outcomes (fills, day-supply) are the cleaner targets.
- **Beneficiary suppression.** `Tot_Benes` is suppressed when fewer than 11 beneficiaries fill that drug from that prescriber in that year. Drug-year aggregates are unaffected; prescriber-level analyses lose low-volume rows.
- **2024 catastrophic redesign contaminates OOP outcomes.** The IRA also eliminated the 5% catastrophic coinsurance in 2024 and capped total beneficiary OOP at $2,000 in 2025. Both affect the control group's OOP — but not utilization at this aggregation level. The primary 30-day-fills outcome is robust; OOP outcomes should be read with the 2019–2023 window only.
- **GLP-1 demand shock.** Semaglutide (Ozempic / Rybelsus / Wegovy) and tirzepatide saw large, supply-constrained demand growth from 2022 onward, partially driven by off-label and weight-loss prescribing visible in Part D claims. Sensitivity analysis drops GLP-1 RAs from the control group.
- **Annual granularity.** Monthly resolution would let interrupted-time-series resolve the January 2023 break sharply; the PUF is annual. The CMS Research Identifiable Files (RIF) carry monthly fill dates but require a Data Use Agreement and a fee.
- **Senior Savings Model partial treatment in 2021–2022.** SSM applied to *enrollees* of participating MA-PDs, not to all Medicare insulin claims. The event-study coefficients for 2021 and 2022 capture this partial treatment honestly; the 2023 ATT is the marginal effect of universalizing the cap.

## Status

Pre-publication portfolio analysis — no peer review, no journal submission yet.

## Citation (interim)

> Del Mundo Del Fierro, P. (2026). *The $35 Insulin Cap and Medicare Part D: a difference-in-differences evaluation of the IRA Section 11406 out-of-pocket cap.* Personal portfolio. https://paulinadelmundomd.com/projects/04-medicare-part-d-insulin-cap/analysis.html
