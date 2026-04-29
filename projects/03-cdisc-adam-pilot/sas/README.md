# SAS programs — CDISC Pilot 01 ADaM derivation

This folder holds the **primary** (SAS) half of the double-programming exercise. The R/`{admiral}` half lives in `../analysis.qmd`.

## Environment

Programs are written for and tested on **SAS OnDemand for Academics** (free, browser-based, registers at <https://welcome.oda.sas.com/>). They will also run on Base SAS 9.4 / SAS Viya without modification — they use only `BASE`, `STAT`, and `ODS`, no `IML`, no `ACCESS`, no Enterprise Guide projects.

## Run order

| # | Program | Reads | Writes |
|---|---|---|---|
| 1 | `setup.sas` | — | `LIBNAME SDTM`, `LIBNAME ADAM`, format catalog |
| 2 | `adsl.sas` | SDTM.DM, SDTM.EX, SDTM.SV, SDTM.SUPPDM | ADAM.ADSL |
| 3 | `adae.sas` | SDTM.AE, ADAM.ADSL | ADAM.ADAE |
| 4 | `adlbc.sas` | SDTM.LB, ADAM.ADSL | ADAM.ADLBC |
| 5 | `t_14_2_01_demog.sas` | ADAM.ADSL | `output/t_14_2_01_demog.rtf` |
| 6 | `xpt_export.sas` | ADAM.ADSL, ADAM.ADAE, ADAM.ADLBC | `output/*.xpt` (v5 transport) |

`xpt_export.sas` writes the v5 transport files the Quarto reconciliation chunk reads back in.

## Conventions

- Two-space indentation, lowercase keywords, dataset names uppercase to match CDISC convention.
- Every derivation is commented with the ADaM IG section it implements.
- Logs are committed to `output/logs/` so the SAS half is auditable even without a SAS install.

## Why SAS at all

The FDA's Study Data Technical Conformance Guide still names SAS V5 transport (XPT) as the required submission format, and the historical reviewer infrastructure runs SAS. Most sponsor SOPs require the *primary* analysis programs to be SAS. R via `{admiral}` is increasingly accepted as the *secondary* check, but a programmer who can only do one of the two is half-equipped. This folder demonstrates the SAS half.
