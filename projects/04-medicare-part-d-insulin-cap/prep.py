"""Local prep for project 04: Medicare Part D insulin-cap difference-in-differences.

Streams the per-year "Medicare Part D Prescribers - by Provider and Drug" public
use files with DuckDB, filters to insulin and a curated set of non-insulin
antihyperglycemic comparators, attaches a drug-class label, and writes three
compact Parquet files that the Quarto analysis consumes.

Run this once locally after placing the raw files under ``data/raw/``:

    python prep.py

Inputs expected under data/raw/ (six files, DY2019-DY2024):
    MUP_DPR_RY*_P04_V*_DY2019.csv
    MUP_DPR_RY*_P04_V*_DY2020.csv
    MUP_DPR_RY*_P04_V*_DY2021.csv
    MUP_DPR_RY*_P04_V*_DY2022.csv
    MUP_DPR_RY*_P04_V*_DY2023.csv
    MUP_DPR_RY*_P04_V*_DY2024.csv

These are the standard CMS filenames; ``DY{year}`` carries the data year and
``RY{year}`` the release year. The script extracts the data year from the
filename via regex, so any release-year version of a given data year works.

Outputs (committed to git; each well under GitHub's 100 MB push limit):
    data/processed/drug_year_panel.parquet           - the primary DiD panel:
        one row per (gnrc_name, year) with totals across all prescribers.
    data/processed/prescriber_drug_year_panel.parquet - prescriber x drug x year
        for prescriber-FE robustness and specialty heterogeneity.
    data/processed/drug_classification.parquet       - generic name -> drug class
        and treatment-arm label, frozen for reproducibility.

Data source:
    CMS Medicare Part D Prescribers - by Provider and Drug
    https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

import duckdb

HERE = pathlib.Path(__file__).parent
RAW = HERE / "data" / "raw"
OUT = HERE / "data" / "processed"

# Non-insulin antihyperglycemics carried as the DiD control group. CMS Part D
# files report the generic stem in upper case; we match case-insensitively in
# DuckDB. Keeping this list explicit (rather than a regex over an ATC class)
# makes the comparator definition auditable and freezes it across renders.
NONINSULIN_GENERICS = [
    # Biguanides
    "METFORMIN HCL",
    "METFORMIN HCL ER",
    # Sulfonylureas
    "GLIPIZIDE",
    "GLIPIZIDE ER",
    "GLYBURIDE",
    "GLYBURIDE MICRONIZED",
    "GLIMEPIRIDE",
    # Thiazolidinediones
    "PIOGLITAZONE HCL",
    "ROSIGLITAZONE MALEATE",
    # DPP-4 inhibitors
    "SITAGLIPTIN PHOSPHATE",
    "SAXAGLIPTIN HCL",
    "LINAGLIPTIN",
    "ALOGLIPTIN BENZOATE",
    # GLP-1 receptor agonists (and the dual GIP/GLP-1 tirzepatide)
    "SEMAGLUTIDE",
    "LIRAGLUTIDE",
    "DULAGLUTIDE",
    "EXENATIDE",
    "EXENATIDE MICROSPHERES",
    "TIRZEPATIDE",
    # SGLT2 inhibitors
    "EMPAGLIFLOZIN",
    "DAPAGLIFLOZIN PROPANEDIOL",
    "CANAGLIFLOZIN",
    "ERTUGLIFLOZIN PIDOLATE",
]

# Insulin captured by the cap is the *pharmaceutical product* — the drug
# substance itself, not the hardware that delivers it. The CMS Part D PUF
# carries pumps, cartridges, reusable pens, syringes, needles, and even
# IV-fluid mixes in the same Gnrc_Name namespace as insulin drugs, and many
# of those DME items happen to contain the word INSULIN. We use a tighter
# rule for the *treated* arm and audit it explicitly.
#
# Treatment criteria:
#   - Generic name starts with 'INSULIN ' (a space — so 'INSULIN' is the
#     active-ingredient stem, not part of a longer hardware noun like
#     'INSULIN PUMP CART').
#   - And does NOT contain 'PUMP' (pump cartridges), is not 'INSULIN PEN'
#     (reusable injector hardware), and does not contain 'NACL' (hospital
#     IV-fluid mixes that show up rarely in the PUF).
#
# Items captured by the loose row-level filter that fail these criteria stay
# in the panel with drug_class = 'Other' and are dropped at the analysis
# stage. This leaves an audit trail.
INSULIN_TREATED_SQL = (
    "gnrc_name LIKE 'INSULIN %' "
    "AND gnrc_name NOT LIKE '%PUMP%' "
    "AND gnrc_name NOT LIKE 'INSULIN PEN%' "
    "AND gnrc_name NOT LIKE '%NACL%'"
)


def _list_year_files() -> list[tuple[int, pathlib.Path]]:
    """Return [(year, path)] for every CMS Part D 'by Provider and Drug' CSV
    found under data/raw/. The data year is extracted from the filename's
    DY{year} segment. CMS uses 2-digit data years in filenames (DY22 = 2022);
    we accept 4-digit forms too in case a future release switches conventions."""
    pat = re.compile(r"DY(\d{2,4})", re.IGNORECASE)
    found: dict[int, pathlib.Path] = {}
    for path in sorted(RAW.glob("MUP_DPR*.csv")):
        m = pat.search(path.name)
        if not m:
            continue
        raw = int(m.group(1))
        # 2-digit -> 20YY. The Part D Prescriber dataset starts in DY13;
        # nothing older exists, so any 2-digit value maps cleanly to 2000s.
        year = raw if raw >= 1000 else 2000 + raw
        # If multiple release years are present for the same data year,
        # prefer the most recently modified (typically the latest revision).
        if year not in found or path.stat().st_mtime > found[year].stat().st_mtime:
            found[year] = path
    return sorted(found.items())


def build_filtered_panel(con: duckdb.DuckDBPyConnection,
                         year_files: list[tuple[int, pathlib.Path]]) -> None:
    """Stream every per-year CSV in turn, filter to the diabetes drugs of
    interest, and append to a single typed table. Reading one file at a time
    keeps memory bounded; using union_by_name avoids brittleness against
    minor column-order or column-name drift across CMS releases."""
    print(f"Filtering {len(year_files)} year files to diabetes drugs ...")
    con.execute("DROP TABLE IF EXISTS pdx_filtered")
    quoted_noninsulin = ", ".join(f"'{g}'" for g in NONINSULIN_GENERICS)

    for i, (year, path) in enumerate(year_files):
        print(f"  [{i+1}/{len(year_files)}] DY{year}: {path.name}")
        select_sql = f"""
            SELECT
              {year}::INTEGER                              AS year,
              CAST(Prscrbr_NPI AS VARCHAR)                 AS npi,
              UPPER(TRIM(Prscrbr_Type))                    AS prscrbr_type,
              UPPER(TRIM(Prscrbr_State_Abrvtn))            AS state,
              UPPER(TRIM(Brnd_Name))                       AS brnd_name,
              UPPER(TRIM(Gnrc_Name))                       AS gnrc_name,
              CAST(Tot_Clms          AS DOUBLE)            AS tot_clms,
              CAST(Tot_30day_Fills   AS DOUBLE)            AS tot_30day_fills,
              CAST(Tot_Day_Suply     AS DOUBLE)            AS tot_day_suply,
              CAST(Tot_Drug_Cst      AS DOUBLE)            AS tot_drug_cst,
              TRY_CAST(Tot_Benes     AS DOUBLE)            AS tot_benes
            FROM read_csv_auto('{path.as_posix()}',
                               sample_size=-1,
                               union_by_name=true,
                               ignore_errors=true)
            WHERE UPPER(TRIM(Gnrc_Name)) LIKE '%INSULIN%'
               OR UPPER(TRIM(Gnrc_Name)) IN ({quoted_noninsulin})
        """
        if i == 0:
            con.execute(f"CREATE TABLE pdx_filtered AS {select_sql}")
        else:
            con.execute(f"INSERT INTO pdx_filtered {select_sql}")

    n = con.execute("SELECT COUNT(*) FROM pdx_filtered").fetchone()[0]
    print(f"  -> pdx_filtered has {n:,} rows")


def add_drug_classification(con: duckdb.DuckDBPyConnection) -> None:
    """Classify each generic name into a drug class and a treatment arm.
    Insulin-containing products are treated; everything else is control. The
    drug-class label is used for sensitivity analyses (e.g. dropping GLP-1
    RAs to remove the concurrent semaglutide / tirzepatide demand shock)."""
    print("Classifying generics into drug classes and treatment arms ...")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE drug_classification AS
        SELECT DISTINCT
          gnrc_name,
          CASE
            WHEN {INSULIN_TREATED_SQL} THEN 'Insulin'
            WHEN gnrc_name IN ('METFORMIN HCL', 'METFORMIN HCL ER')
              THEN 'Biguanide'
            WHEN gnrc_name IN ('GLIPIZIDE', 'GLIPIZIDE ER', 'GLYBURIDE',
                               'GLYBURIDE MICRONIZED', 'GLIMEPIRIDE')
              THEN 'Sulfonylurea'
            WHEN gnrc_name IN ('PIOGLITAZONE HCL', 'ROSIGLITAZONE MALEATE')
              THEN 'Thiazolidinedione'
            WHEN gnrc_name IN ('SITAGLIPTIN PHOSPHATE', 'SAXAGLIPTIN HCL',
                               'LINAGLIPTIN', 'ALOGLIPTIN BENZOATE')
              THEN 'DPP-4 inhibitor'
            WHEN gnrc_name IN ('SEMAGLUTIDE', 'LIRAGLUTIDE', 'DULAGLUTIDE',
                               'EXENATIDE', 'EXENATIDE MICROSPHERES',
                               'TIRZEPATIDE')
              THEN 'GLP-1 receptor agonist'
            WHEN gnrc_name IN ('EMPAGLIFLOZIN', 'DAPAGLIFLOZIN PROPANEDIOL',
                               'CANAGLIFLOZIN', 'ERTUGLIFLOZIN PIDOLATE')
              THEN 'SGLT2 inhibitor'
            ELSE 'Other'
          END                                                   AS drug_class,
          CASE WHEN {INSULIN_TREATED_SQL} THEN 1 ELSE 0 END      AS treated
        FROM pdx_filtered
        """
    )


def write_outputs(con: duckdb.DuckDBPyConnection) -> None:
    """Three Parquets: a small drug-year panel for the primary DiD, a larger
    prescriber-drug-year panel for FE robustness, and the classification table
    so the analysis can join class labels without re-deriving them."""
    OUT.mkdir(parents=True, exist_ok=True)

    print(f"Writing {OUT / 'drug_classification.parquet'} ...")
    con.execute(
        f"""
        COPY drug_classification
        TO '{(OUT / "drug_classification.parquet").as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    print(f"Writing {OUT / 'drug_year_panel.parquet'} ...")
    con.execute(
        f"""
        COPY (
          SELECT
            f.year,
            f.gnrc_name,
            c.drug_class,
            c.treated,
            COUNT(DISTINCT f.npi)               AS prescribers_n,
            SUM(f.tot_clms)                     AS tot_clms,
            SUM(f.tot_30day_fills)              AS tot_30day_fills,
            SUM(f.tot_day_suply)                AS tot_day_suply,
            SUM(f.tot_drug_cst)                 AS tot_drug_cst,
            SUM(f.tot_benes)                    AS tot_benes_sum,
            SUM(CASE WHEN f.tot_benes IS NULL THEN 1 ELSE 0 END) AS rows_suppressed
          FROM pdx_filtered f
          JOIN drug_classification c USING (gnrc_name)
          GROUP BY 1, 2, 3, 4
          ORDER BY gnrc_name, year
        )
        TO '{(OUT / "drug_year_panel.parquet").as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    print(f"Writing {OUT / 'prescriber_drug_year_panel.parquet'} ...")
    con.execute(
        f"""
        COPY (
          SELECT
            f.year,
            f.npi,
            f.prscrbr_type,
            f.state,
            f.gnrc_name,
            c.drug_class,
            c.treated,
            f.tot_clms,
            f.tot_30day_fills,
            f.tot_day_suply,
            f.tot_drug_cst,
            f.tot_benes
          FROM pdx_filtered f
          JOIN drug_classification c USING (gnrc_name)
        )
        TO '{(OUT / "prescriber_drug_year_panel.parquet").as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )


def main() -> None:
    # Declared up front so the override below doesn't trip Python's
    # "name used prior to global" rule.
    global RAW

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--raw", default=str(RAW),
                    help="Directory holding the per-year MUP_DPR*.csv files. "
                         f"Defaults to {RAW}.")
    args = ap.parse_args()

    raw_dir = pathlib.Path(args.raw)
    if not raw_dir.exists():
        sys.exit(f"Raw directory does not exist: {raw_dir}")

    # Repoint the module-level RAW so _list_year_files reads the right place
    # if the user overrode --raw on the command line.
    RAW = raw_dir

    year_files = _list_year_files()
    if not year_files:
        sys.exit(
            f"No files matching MUP_DPR*.csv in {raw_dir}. Download the per-year "
            "'Medicare Part D Prescribers - by Provider and Drug' CSVs from "
            "https://data.cms.gov/provider-summary-by-type-of-service/"
            "medicare-part-d-prescribers and place them here."
        )

    years = [y for y, _ in year_files]
    print(f"Found {len(year_files)} year files: {years}")

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    con.execute("PRAGMA memory_limit='16GB'")

    build_filtered_panel(con, year_files)
    add_drug_classification(con)
    write_outputs(con)

    print("Done.")


if __name__ == "__main__":
    main()
