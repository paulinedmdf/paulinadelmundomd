"""Local prep for project 02: Medicaid provider outlier detection.

Streams the 11 GB Medicaid Provider Spending CSV and the ~45 GB unzipped NPPES
bulk CSV with DuckDB, joins them to NUCC taxonomy and US Census ZCTA centroids,
and writes three compact Parquet files that the Quarto analysis consumes.

Run this once locally after placing the raw files under ``data/raw/``:

    python prep.py

Outputs (committed to git; each under GitHub's 100 MB push limit):
    data/processed/state_group_atlas.parquet  - state x disease group with
        total paid, total beneficiaries, paid-per-beneficiary, provider count,
        and within-group quintile rank for choropleth shading.
    data/processed/group_totals.parquet       - national rollup per disease group.
    data/processed/group_top_codes.parquet    - top 5 HCPCS codes by paid
        within each disease group, with descriptions.
    data/processed/outliers.parquet           - NPI x HCPCS rows with robust_z >= 2
        OR within the top 20 HCPCS codes by spend. Kept for one deep-dive map.
    data/processed/hcpcs_summary.parquet      - HCPCS-level totals + median/MAD.
    data/processed/state_summary.parquet      - state-level aggregates.
    data/processed/specialty_summary.parquet  - specialty quantiles.
    data/processed/taxonomy.parquet           - NUCC taxonomy code -> specialty.
    data/processed/zip_centroids.parquet      - ZCTA -> lat/lon (US Census).

Data sources:
    Medicaid Provider Spending
      https://opendata.hhs.gov/datasets/medicaid-provider-spending/
    NPPES Downloadable File
      https://download.cms.gov/nppes/NPI_Files.html
    NUCC Health Care Provider Taxonomy
      https://taxonomy.nucc.org/
    US Census ZCTA Gazetteer (2020)
      https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html
"""

from __future__ import annotations

import argparse
import pathlib
import sys

import duckdb
import requests

HERE = pathlib.Path(__file__).parent
RAW = HERE / "data" / "raw"
OUT = HERE / "data" / "processed"

NUCC_URL = "https://www.nucc.org/images/stories/CSV/nucc_taxonomy_250.csv"
CENSUS_ZCTA_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2020_Gazetteer/"
    "2020_Gaz_zcta_national.zip"
)
CENSUS_ZCTA_COUNTY_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/"
    "tab20_zcta520_county20_natl.txt"
)


def _pick(pattern: str) -> pathlib.Path:
    matches = sorted(RAW.glob(pattern))
    if not matches:
        sys.exit(f"Missing input: no file matching {pattern!r} in {RAW}")
    return matches[-1]


def download(url: str, dest: pathlib.Path) -> pathlib.Path:
    if dest.exists():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {url} -> {dest}")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with dest.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    return dest


def build_medicaid_aggregate(con: duckdb.DuckDBPyConnection, medicaid_csv: pathlib.Path) -> None:
    print("Aggregating Medicaid claims to NPI x HCPCS ...")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE provider_hcpcs AS
        SELECT
          BILLING_PROVIDER_NPI_NUM::VARCHAR         AS npi,
          HCPCS_CODE                                AS hcpcs,
          SUM(TOTAL_UNIQUE_BENEFICIARIES)::BIGINT   AS beneficiaries,
          SUM(TOTAL_CLAIMS)::BIGINT                 AS claims,
          SUM(TOTAL_PAID)::DOUBLE                   AS paid,
          COUNT(DISTINCT CLAIM_FROM_MONTH)          AS months_active
        FROM read_csv_auto('{medicaid_csv.as_posix()}', sample_size=-1)
        GROUP BY 1, 2
        """
    )


def add_robust_z(con: duckdb.DuckDBPyConnection, min_bene: int = 11) -> None:
    """Within each HCPCS peer group, compute a MAD-based robust z-score
    for paid-per-beneficiary. Scale factor 1.4826 makes MAD -> sigma for
    a normal distribution."""
    print("Computing peer-group robust z-scores ...")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE provider_scored AS
        WITH base AS (
          SELECT npi, hcpcs, beneficiaries, claims, paid, months_active,
                 paid / NULLIF(beneficiaries, 0) AS paid_per_bene
          FROM provider_hcpcs
          WHERE beneficiaries >= {min_bene}
        ),
        med AS (
          SELECT hcpcs, MEDIAN(paid_per_bene) AS med,
                 COUNT(*) AS peer_n
          FROM base
          GROUP BY hcpcs
          HAVING COUNT(*) >= 25
        ),
        mad_cte AS (
          SELECT b.hcpcs, MEDIAN(ABS(b.paid_per_bene - m.med)) AS mad
          FROM base b JOIN med m USING (hcpcs)
          GROUP BY b.hcpcs
        )
        SELECT
          b.npi, b.hcpcs, b.beneficiaries, b.claims, b.paid, b.months_active,
          b.paid_per_bene,
          m.med                                           AS hcpcs_median_ppb,
          a.mad                                           AS hcpcs_mad_ppb,
          m.peer_n                                        AS hcpcs_peer_n,
          CASE WHEN a.mad = 0 THEN NULL
               ELSE (b.paid_per_bene - m.med) / (1.4826 * a.mad)
          END                                             AS robust_z
        FROM base b
        JOIN med m     USING (hcpcs)
        JOIN mad_cte a USING (hcpcs)
        """
    )


def join_nppes(con: duckdb.DuckDBPyConnection, nppes_csv: pathlib.Path) -> None:
    print("Joining to NPPES registry (state, zip, taxonomy) ...")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE nppes AS
        SELECT
          NPI::VARCHAR                                               AS npi,
          "Entity Type Code"                                         AS entity_type,
          "Provider Business Practice Location Address State Name"   AS state,
          LEFT("Provider Business Practice Location Address Postal Code", 5) AS zip,
          "Healthcare Provider Taxonomy Code_1"                      AS taxonomy
        FROM read_csv_auto('{nppes_csv.as_posix()}', sample_size=-1, ignore_errors=true)
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE provider_enriched AS
        SELECT s.*, n.entity_type, n.state, n.zip, n.taxonomy
        FROM provider_scored s
        LEFT JOIN nppes n USING (npi)
        """
    )


def join_taxonomy_and_geo(con: duckdb.DuckDBPyConnection, nucc_csv: pathlib.Path,
                          zcta_tsv: pathlib.Path) -> None:
    print("Attaching specialty names and zip centroids ...")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE taxonomy AS
        SELECT
          Code          AS taxonomy,
          Classification AS specialty_class,
          Specialization AS specialty_sub,
          "Display Name" AS specialty_name
        FROM read_csv_auto('{nucc_csv.as_posix()}', sample_size=-1)
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE zip_centroids AS
        SELECT
          GEOID::VARCHAR AS zip,
          INTPTLAT::DOUBLE AS lat,
          INTPTLONG::DOUBLE AS lon
        FROM read_csv_auto('{zcta_tsv.as_posix()}', sep='\t', sample_size=-1)
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE provider_full AS
        SELECT e.*,
               t.specialty_name, t.specialty_class, t.specialty_sub,
               z.lat, z.lon
        FROM provider_enriched e
        LEFT JOIN taxonomy      t USING (taxonomy)
        LEFT JOIN zip_centroids z USING (zip)
        """
    )


def join_county_and_disease_group(con: duckdb.DuckDBPyConnection,
                                   zcta_county_file: pathlib.Path) -> None:
    """Add a ZIP -> county_fips mapping (Census 2020 ZCTA relationship file) and
    a rule-based HCPCS -> disease_group mapping, then attach both to
    provider_full so downstream aggregates can roll up by county × group."""
    print("Attaching ZIP -> county and HCPCS -> disease group ...")

    con.execute(
        f"""
        CREATE OR REPLACE TABLE zcta_to_county AS
        WITH raw AS (
          SELECT
            GEOID_ZCTA5_20::VARCHAR                   AS zip,
            GEOID_COUNTY_20::VARCHAR                  AS county_fips,
            NAMELSAD_COUNTY_20                        AS county_name,
            AREALAND_PART                             AS area_part,
            ROW_NUMBER() OVER (
              PARTITION BY GEOID_ZCTA5_20
              ORDER BY AREALAND_PART DESC
            )                                         AS rn
          FROM read_csv_auto('{zcta_county_file.as_posix()}', sep='|', sample_size=-1)
        )
        SELECT zip, county_fips, county_name
        FROM raw
        WHERE rn = 1
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE hcpcs_group AS
        SELECT DISTINCT hcpcs,
          CASE
            WHEN hcpcs LIKE 'J%' THEN 'Prescription Drugs & Biologics'
            WHEN hcpcs LIKE 'D%' THEN 'Dental'
            WHEN hcpcs LIKE 'H%' THEN 'Behavioral Health'
            WHEN hcpcs LIKE 'E%' OR hcpcs LIKE 'K%' OR hcpcs LIKE 'L%'
              THEN 'Durable Medical Equipment & Supplies'
            WHEN regexp_matches(hcpcs, '^T(20|21|22|51)')
              OR hcpcs LIKE 'A04%' THEN 'Transportation'
            WHEN hcpcs LIKE 'T%' THEN 'Long-Term Services & Supports'
            WHEN hcpcs IN ('G0257','G0323','G0324','G0325','G0326','G0327')
              THEN 'Dialysis & ESRD'
            WHEN regexp_matches(hcpcs, '^[0-9]{5}$') THEN
              CASE
                WHEN CAST(hcpcs AS INTEGER) BETWEEN 90791 AND 90899
                  THEN 'Behavioral Health'
                WHEN CAST(hcpcs AS INTEGER) BETWEEN 90935 AND 90999
                  THEN 'Dialysis & ESRD'
                WHEN CAST(hcpcs AS INTEGER) BETWEEN 99201 AND 99499
                  THEN 'Evaluation & Management'
                WHEN CAST(hcpcs AS INTEGER) BETWEEN 10021 AND 69990
                  THEN 'Surgery & Outpatient Procedures'
                WHEN CAST(hcpcs AS INTEGER) BETWEEN 70010 AND 79999
                  THEN 'Radiology & Imaging'
                WHEN CAST(hcpcs AS INTEGER) BETWEEN 80047 AND 89398
                  THEN 'Laboratory & Diagnostics'
                ELSE 'Other Medical Services'
              END
            ELSE 'Other Medical Services'
          END AS disease_group
        FROM provider_full
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE provider_atlas AS
        SELECT p.*, c.county_fips, c.county_name, g.disease_group
        FROM provider_full p
        LEFT JOIN zcta_to_county c USING (zip)
        LEFT JOIN hcpcs_group    g USING (hcpcs)
        """
    )


def write_outputs(con: duckdb.DuckDBPyConnection) -> None:
    """Emit several small Parquet files rather than one big one, so each fits
    under GitHub's 100 MB push limit. The qmd reads the aggregates for the
    Pareto / state / specialty views and the filtered detail for the map."""
    OUT.mkdir(parents=True, exist_ok=True)

    print(f"Writing {OUT / 'hcpcs_summary.parquet'} ...")
    con.execute(
        f"""
        COPY (
          SELECT hcpcs,
                 ANY_VALUE(hcpcs_peer_n)          AS peer_n,
                 SUM(beneficiaries)               AS total_bene,
                 SUM(paid)                        AS total_paid,
                 ANY_VALUE(hcpcs_median_ppb)      AS median_ppb,
                 ANY_VALUE(hcpcs_mad_ppb)         AS mad_ppb
          FROM provider_full
          WHERE robust_z IS NOT NULL
          GROUP BY hcpcs
        )
        TO '{(OUT / "hcpcs_summary.parquet").as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    print(f"Writing {OUT / 'state_summary.parquet'} ...")
    con.execute(
        f"""
        COPY (
          SELECT state,
                 COUNT(DISTINCT npi)                                    AS providers_n,
                 SUM(paid)                                              AS total_paid,
                 SUM(GREATEST(paid_per_bene - hcpcs_median_ppb, 0)
                     * beneficiaries)                                   AS excess_paid,
                 MEDIAN(robust_z)                                       AS median_z,
                 QUANTILE(robust_z, 0.95)                               AS q95_z,
                 SUM(CASE WHEN robust_z >= 5 THEN 1 ELSE 0 END)         AS flagged_ge_5
          FROM provider_full
          WHERE robust_z IS NOT NULL AND state IS NOT NULL
          GROUP BY state
        )
        TO '{(OUT / "state_summary.parquet").as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    print(f"Writing {OUT / 'specialty_summary.parquet'} ...")
    con.execute(
        f"""
        COPY (
          SELECT specialty_name,
                 specialty_class,
                 COUNT(*)                                               AS rows_n,
                 SUM(CASE WHEN robust_z >= 5 THEN 1 ELSE 0 END)         AS flagged_ge_5,
                 QUANTILE(paid_per_bene, 0.05)                          AS p05,
                 QUANTILE(paid_per_bene, 0.25)                          AS p25,
                 MEDIAN(paid_per_bene)                                  AS p50,
                 QUANTILE(paid_per_bene, 0.75)                          AS p75,
                 QUANTILE(paid_per_bene, 0.95)                          AS p95
          FROM provider_full
          WHERE robust_z IS NOT NULL AND specialty_name IS NOT NULL
          GROUP BY 1, 2
        )
        TO '{(OUT / "specialty_summary.parquet").as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    print(f"Writing {OUT / 'outliers.parquet'} ...")
    con.execute(
        f"""
        COPY (
          SELECT npi, hcpcs, state, specialty_name, specialty_class,
                 beneficiaries, claims, paid, months_active,
                 paid_per_bene, hcpcs_median_ppb, robust_z,
                 lat, lon
          FROM provider_full
          WHERE robust_z IS NOT NULL
            AND (robust_z >= 2
                 OR hcpcs IN (
                   SELECT hcpcs FROM (
                     SELECT hcpcs, SUM(paid) AS p
                     FROM provider_full
                     GROUP BY hcpcs
                     ORDER BY p DESC
                     LIMIT 20
                   )
                 ))
        )
        TO '{(OUT / "outliers.parquet").as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    print(f"Writing {OUT / 'taxonomy.parquet'} ...")
    con.execute(f"COPY taxonomy TO '{(OUT / 'taxonomy.parquet').as_posix()}' (FORMAT PARQUET)")

    print(f"Writing {OUT / 'zip_centroids.parquet'} ...")
    con.execute(f"COPY zip_centroids TO '{(OUT / 'zip_centroids.parquet').as_posix()}' (FORMAT PARQUET)")

    print(f"Writing {OUT / 'county_group_atlas.parquet'} ...")
    con.execute(
        f"""
        COPY (
          WITH base AS (
            SELECT county_fips, county_name, state, disease_group,
                   SUM(paid)          AS total_paid,
                   SUM(beneficiaries) AS total_bene,
                   COUNT(DISTINCT npi) AS providers_n
            FROM provider_atlas
            WHERE county_fips IS NOT NULL
              AND disease_group IS NOT NULL
            GROUP BY 1, 2, 3, 4
          )
          SELECT
            county_fips,
            county_name,
            state,
            disease_group,
            total_paid,
            total_bene,
            providers_n,
            total_paid / NULLIF(total_bene, 0)                              AS paid_per_bene,
            NTILE(5) OVER (
              PARTITION BY disease_group
              ORDER BY total_paid / NULLIF(total_bene, 0)
            )                                                               AS quintile_ppb
          FROM base
        )
        TO '{(OUT / "county_group_atlas.parquet").as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    print(f"Writing {OUT / 'group_totals.parquet'} ...")
    con.execute(
        f"""
        COPY (
          SELECT disease_group,
                 SUM(paid)            AS total_paid,
                 SUM(beneficiaries)   AS total_bene,
                 COUNT(DISTINCT npi)  AS providers_n,
                 COUNT(DISTINCT hcpcs) AS hcpcs_n,
                 SUM(paid) / NULLIF(SUM(beneficiaries), 0) AS paid_per_bene
          FROM provider_atlas
          WHERE disease_group IS NOT NULL
          GROUP BY disease_group
        )
        TO '{(OUT / "group_totals.parquet").as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--medicaid", default=None,
                    help="Path to medicaid-provider-spending.csv. "
                         "Defaults to newest file matching that name under data/raw/.")
    ap.add_argument("--nppes", default=None,
                    help="Path to NPPES npidata_pfile CSV (unzipped).")
    ap.add_argument("--nucc", default=None,
                    help="Path to NUCC taxonomy CSV. Auto-downloaded if omitted.")
    ap.add_argument("--zcta", default=None,
                    help="Path to 2020_Gaz_zcta_national.txt (unzipped tab-delimited).")
    args = ap.parse_args()

    RAW.mkdir(parents=True, exist_ok=True)

    medicaid_csv = pathlib.Path(args.medicaid) if args.medicaid else _pick("medicaid-provider-spending*.csv")
    nppes_csv    = pathlib.Path(args.nppes)    if args.nppes    else _pick("npidata_pfile*.csv")
    nucc_csv     = pathlib.Path(args.nucc)     if args.nucc     else download(NUCC_URL, RAW / "nucc_taxonomy.csv")

    if args.zcta:
        zcta_tsv = pathlib.Path(args.zcta)
    else:
        zcta_tsv = RAW / "2020_Gaz_zcta_national.txt"
        if not zcta_tsv.exists():
            import io, zipfile
            print(f"Fetching Census ZCTA gazetteer from {CENSUS_ZCTA_URL}")
            r = requests.get(CENSUS_ZCTA_URL, timeout=180)
            r.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                members = [m for m in zf.namelist() if m.endswith(".txt")]
                if not members:
                    sys.exit("Census ZCTA zip did not contain a .txt file")
                zcta_tsv.write_bytes(zf.read(members[0]))

    zcta_county_file = download(CENSUS_ZCTA_COUNTY_URL, RAW / "zcta_county_relationship.txt")

    print(f"Medicaid : {medicaid_csv}")
    print(f"NPPES    : {nppes_csv}")
    print(f"NUCC     : {nucc_csv}")
    print(f"ZCTA     : {zcta_tsv}")

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    con.execute("PRAGMA memory_limit='16GB'")

    build_medicaid_aggregate(con, medicaid_csv)
    add_robust_z(con)
    join_nppes(con, nppes_csv)
    join_taxonomy_and_geo(con, nucc_csv, zcta_tsv)
    join_county_and_disease_group(con, zcta_county_file)
    write_outputs(con)

    print("Done.")


if __name__ == "__main__":
    main()
