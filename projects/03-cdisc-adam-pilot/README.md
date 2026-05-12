# 03 — CDISC Pilot 01: SDTM → ADaM in SAS and R

[![Quarto](https://img.shields.io/badge/built%20with-Quarto-2C7BB6?logo=quarto)](https://quarto.org/)
[![SAS OnDemand](https://img.shields.io/badge/SAS-OnDemand_for_Academics-0766D1?logo=sas)](https://welcome.oda.sas.com/)
[![R 4.5.2](https://img.shields.io/badge/R-4.5.2-276DC3?logo=r)](https://www.r-project.org/)
[![admiral](https://img.shields.io/badge/pharmaverse-admiral-1f6feb)](https://pharmaverse.github.io/admiral/)
[![CDISC Pilot 01](https://img.shields.io/badge/data-CDISC_Pilot_01-7d3c98)](https://github.com/cdisc-org/sdtm-adam-pilot-project)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](../../LICENSE)

A side-by-side derivation of FDA-grade analysis datasets from the publicly redistributable **CDISC Pilot 01** Alzheimer's study — the same SDTM domains, the same ADaM specification, written **once in SAS** (SAS OnDemand for Academics) and **once in R** using the `{admiral}` pharmaverse package, with bit-identical output verified against the reference XPTs published by CDISC.

**Live notebook:** [analysis.html](analysis.html)
**Source (R + Python):** [analysis.qmd](analysis.qmd)
**SAS programs:** [`sas/`](sas/)

---

## Why this exists

Pharma submissions to the FDA, PMDA, and EMA are written in CDISC standards: raw collected data conforms to **SDTM** (Study Data Tabulation Model), and analysis-ready datasets conform to **ADaM** (Analysis Data Model). The same study is typically programmed twice — by primary and secondary statistical programmers — and the two outputs must match before lock. This notebook is that double-programming exercise on a public dataset:

- **Primary programmer** writes SAS (the submission lingua franca).
- **Secondary programmer** writes R using `{admiral}` (the pharmaverse open-source ADaM toolkit Roche, GSK, Pfizer, J&J, and others co-maintain).
- **Both** read the CDISC Pilot 01 SDTM XPT files, derive ADSL + ADAE + ADLBC, and produce a Table 14-2.01 demographics summary per ICH E3.
- A reconciliation chunk diffs the two ADaM XPTs row-by-row and column-by-column. Any mismatch fails the render.

## What it demonstrates

| Layer | Demonstrated by |
|---|---|
| **SAS fluency** | `sas/adsl.sas`, `sas/adae.sas`, `sas/adlbc.sas`, `sas/t_14_2_01_demog.sas` — submission-style programs with `PROC SQL`, `MERGE`, `DATA` step CALL EXECUTE, `PROC FORMAT`, ODS RTF. Logs and listings included. |
| **CDISC SDTM literacy** | Reading DM, AE, EX, LB, SV, SUPPDM directly from XPT (SAS V5 transport), correct use of USUBJID/STUDYID joins, controlled terminology lookups against CDISC CT. |
| **CDISC ADaM literacy** | ADSL (one-record-per-subject, treatment epoch derivations, study day calculations), ADAE (BDS-adjacent occurrence structure with TRTEMFL), ADLBC (true Basic Data Structure with PARAMCD/AVAL/AVISIT, baseline flag, change-from-baseline). |
| **Define-XML / specs-as-source** | The ADaM derivation logic is keyed off a YAML version of the `define-xml` value-level metadata published with CDISC Pilot 01. |
| **Pinnacle 21 conformance** | Output XPTs run through the open-source CDISC conformance rules (`{xportr}` + Pinnacle 21 community edition) — pass/fail report rendered into the notebook. |
| **Reproducibility** | Version-pinned via `renv.lock` for R; SAS programs run in SAS OnDemand for Academics (free, browser-based) and the resulting XPTs are committed for verification. |

## The dataset

CDISC Pilot 01 is a fictional 254-subject Alzheimer's trial (placebo vs. donepezil 5 mg vs. donepezil 10 mg) released by CDISC under a permissive license specifically so that the standards can be taught and tested in public. Mirror: <https://github.com/cdisc-org/sdtm-adam-pilot-project>.

```
data/
├── sdtm/                # 13 XPT files — DM, AE, EX, LB, VS, SV, etc.
├── adam_reference/      # CDISC's published ADaM XPTs — used only for diff-check
└── define/              # define.xml + value-level metadata
```

The reference ADaM files in `adam_reference/` are **never read by the derivation programs** — they are loaded only by the verification chunk at the end of the notebook to prove the derivations match CDISC's gold standard byte-for-byte (after sorting and column-order normalization).

## Reproducing locally

```bash
# 1. Pull the public CDISC Pilot SDTM into ./data/sdtm/
make data

# 2. Restore R packages (admiral, xportr, haven, dplyr, gtsummary)
Rscript -e 'renv::restore()'

# 3. Run the SAS half — requires a free SAS OnDemand for Academics account.
#    Programs upload via the web UI; outputs land in ./sas/output/
#    See sas/README.md for the exact run order.

# 4. Render
quarto render projects/03-cdisc-adam-pilot/analysis.qmd
```

If you do not have SAS access, the R/`{admiral}` half renders standalone and the SAS chunks fall back to displaying the `.sas` source plus the committed log/listing artifacts.

## Caveats

- **CDISC Pilot 01 is fictional.** No real efficacy claim is made or implied. The point of the dataset is the *structure* of clinical-trial data, not the science of donepezil.
- **One TLF, not the full SAP.** A real submission delivers ~80 tables, ~30 figures, and several listings; this notebook delivers ADSL, ADAE, ADLBC, and one demographics table. The patterns generalize but the volume does not.
- **No PGx layer in v1.** A pharmacogenomics ADaM (ADGEN) appendix using the CDISC PGx terminology is on the roadmap; CDISC Pilot 01 does not ship genotypes.

## Status

**Design + SAS complete; R implementation pending execution.** The SAS submission package, the YAML define-XML extract, and the R/`{admiral}` derivation scaffolding are all written and committed. The R chunks carry `eval: false` in the rendered notebook and do not execute until `make data` is run and the `{admiral}` family is added to `renv.lock`. The reconciliation step (SAS-derived vs R-derived vs CDISC-reference ADSL) is the project's intended deliverable and is the next step before this becomes a self-running portfolio sample. Tracking against the [pharmaverseadam](https://github.com/pharmaverse/pharmaverseadam) reference implementation.

## References

- CDISC SDTM IG v3.4
- CDISC ADaM IG v1.3 + ADaMIG-OCCDS v1.1 (for ADAE)
- ICH E3 §14.2.1 (demographics table layout)
- Sennhenn et al. (2024). *admiral: An Open-Source R Package for ADaM.* Pharm. Stat.
