# 02 — Outlier Detection in Medicaid Provider Spending

[![Quarto](https://img.shields.io/badge/built%20with-Quarto-2C7BB6?logo=quarto)](https://quarto.org/)
[![R 4.5.2](https://img.shields.io/badge/R-4.5.2-276DC3?logo=r)](https://www.r-project.org/)
[![DuckDB](https://img.shields.io/badge/streaming-DuckDB-FFF000)](https://duckdb.org/)
[![reproducible: renv](https://img.shields.io/badge/reproducible-renv-2C7B6E)](../../renv.lock)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](../../LICENSE)

A laptop-scale analysis of the HHS Medicaid Provider Spending file (11 GB CSV) using DuckDB for streaming aggregation, MAD-based robust z-scores with BH-FDR multiplicity control, an isolation-forest second opinion, and a county-level interactive cost atlas across all 12 HCPCS spending categories.

**Live notebook:** [analysis.html](analysis.html) (60 MB — embeds 12 layers of county polygons)
**Source:** [analysis.qmd](analysis.qmd)
**Prep pipeline:** [prep.py](prep.py)

---

## Headline findings

- **Heavy tails are the regime, not the exception.** Paid-per-beneficiary spans four orders of magnitude on a log scale — exactly why mean/SD z's fail and MAD-based robust z's work.
- **BH-FDR ≤ 0.01 keeps a much smaller, much higher-confidence flag list than raw z ≥ 5.** The intersection of the two rules is the set a Program Integrity team should review first.
- **The 12-bucket cost atlas is an HCPCS lens, not a disease lens.** "Behavioral Health spending geography" is supportable; "depression cost geography" is not — HCPCS doesn't carry diagnosis.
- **Spatial concentration varies sharply by category.** Dialysis & ESRD and parts of DME concentrate geographically (high Gini); LTSS and E&M diffuse across the country (low Gini) and shouldn't be read as having "hotspots."

## Methods

| Layer | Approach |
|---|---|
| Source | Medicaid Provider Spending (HHS Open Data Portal, monthly) — 11 GB CSV |
| Storage | DuckDB streaming aggregate to NPI × HCPCS Parquet; committed artifacts under 100 MB each |
| Outlier rule | MAD-based robust z per HCPCS peer group (1.4826 scale) |
| Multiplicity | Benjamini–Hochberg FDR ≤ 0.01 on two-sided p from the robust z |
| Unsupervised | Isolation forest on 4 standardized features with collinearity diagnostics (Pearson correlation matrix, condition number, approximate VIFs) |
| Geography | NPPES → ZCTA → county FIPS via Census 2020 ZCTA relationship file |
| Categorization | Rule-based HCPCS → 12 spending categories (`classify_hcpcs()` mirrors the prep CASE) |
| Map | Leaflet choropleth, within-category decile shading (10-step purple ramp), 12-option dropdown |
| Explorer | Top-5 HCPCS / spatial Gini + top-10-county share / state hotspot table — all 12 categories in one pass |

## Reproducing locally

```bash
# 1. Place the raw inputs under projects/02-medicaid-outliers/data/raw/
#    - medicaid-provider-spending.csv  (11 GB; HHS Open Data Portal)
#    - npidata_pfile_*.csv             (45 GB; CMS NPPES bulk file)

# 2. Run the DuckDB prep — emits the 7 committed Parquets in data/processed/
python projects/02-medicaid-outliers/prep.py

# 3. Restore the R package versions and render
Rscript -e 'renv::restore()'
quarto render projects/02-medicaid-outliers/analysis.qmd
```

The .qmd reads only the committed Parquets, so re-rendering doesn't require the raw inputs once prep.py has been run once.

## Caveats

- **Peer group is too broad** — currently "everyone billing this HCPCS" mixes specialties and panel sizes. The publishable peer is `specialty × panel-size band × setting`. Largest single robustness gap.
- **Single monthly drop, no temporal control** — rolling 3-month windows would separate persistent outliers from transient spikes
- **Geocoding gaps** — a fraction of NPPES providers don't resolve to a Census ZCTA centroid; those counties under-represent on the map
- **Category sparsity affects decile shading** — Dialysis & ESRD is billed in ~500 counties, so decile boundaries get visually closer there than for densely-covered categories like E&M

## Status

Pre-publication portfolio analysis — no peer review, no journal submission yet.

## Citation (interim)

> Del Mundo Del Fierro, P. (2026). *Outlier Detection in Medicaid Provider Spending: peer-group robust z-scores with BH-FDR multiplicity control.* Personal portfolio. https://paulinadelmundomd.com/projects/02-medicaid-outliers/analysis.html
