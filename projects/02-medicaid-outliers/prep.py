"""Local prep for project 02: Medicaid provider outlier detection.

Streams the 11 GB Medicaid Provider Spending CSV and the ~45 GB unzipped NPPES
bulk CSV with DuckDB, joins them to NUCC taxonomy and US Census ZCTA centroids,
and writes three compact Parquet files that the Quarto analysis consumes.

Run this once locally after placing the raw files under ``data/raw/``:

    python prep.py

Outputs (committed to git, small):
    data/processed/medicaid_outliers.parquet  - one row per NPI x HCPCS with
        aggregates, MAD-based robust z-score, state, specialty, zip, lat/lon.
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


def write_outputs(con: duckdb.DuckDBPyConnection) -> None:
    OUT.mkdir(parents=True, exist_ok=True)

    out_main = OUT / "medicaid_outliers.parquet"
    print(f"Writing {out_main} ...")
    con.execute(
        f"""
        COPY (
          SELECT * FROM provider_full
          WHERE robust_z IS NOT NULL
        )
        TO '{out_main.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    out_tax = OUT / "taxonomy.parquet"
    print(f"Writing {out_tax} ...")
    con.execute(f"COPY taxonomy TO '{out_tax.as_posix()}' (FORMAT PARQUET)")

    out_zip = OUT / "zip_centroids.parquet"
    print(f"Writing {out_zip} ...")
    con.execute(f"COPY zip_centroids TO '{out_zip.as_posix()}' (FORMAT PARQUET)")


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
    write_outputs(con)

    print("Done.")


if __name__ == "__main__":
    main()
