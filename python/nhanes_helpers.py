"""Shared NHANES data-access helpers for Python analyses.

Download NHANES XPT files directly from the CDC and return a tidy DataFrame.
Used by the Quarto analyses under `projects/`.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyreadstat
import requests

CDC_BASE = "https://wwwcdn.cdc.gov/nchs/nhanes"

CYCLE_URL = {
    "J": "2017-2018",
    "I": "2015-2016",
    "H": "2013-2014",
    "G": "2011-2012",
}


def _download(table: str, cycle: str, cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    local = cache_dir / f"{table}.xpt"
    if local.exists():
        return local
    url = f"{CDC_BASE}/{CYCLE_URL[cycle]}/{table}.XPT"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    local.write_bytes(r.content)
    return local


def pull_nhanes_cycle(
    cycle: str,
    tables: dict[str, str],
    cache_dir: str | Path = "data",
) -> pd.DataFrame:
    """Pull and merge NHANES tables for a single cycle, joined on SEQN.

    Parameters
    ----------
    cycle : str
        One-letter suffix ("J" = 2017-2018, "I" = 2015-2016, ...).
    tables : dict[str, str]
        Mapping of logical name -> NHANES file stem (e.g., {"DEMO": "DEMO_J"}).
    cache_dir : str | Path
        Where to cache .xpt downloads locally (gitignored).
    """
    cache = Path(cache_dir)
    frames = []
    for _, stem in tables.items():
        path = _download(stem, cycle, cache)
        df, _ = pyreadstat.read_xport(str(path))
        frames.append(df)
    out = frames[0]
    for df in frames[1:]:
        out = out.merge(df, on="SEQN", how="outer")
    return out


RACE_LABELS = {
    1: "Mexican American",
    2: "Other Hispanic",
    3: "NH White",
    4: "NH Black",
    6: "NH Asian",
    7: "Other / Multi",
}


def recode_demographics(df: pd.DataFrame) -> pd.DataFrame:
    """Recode NHANES demographic columns to analysis-friendly values."""
    out = df.copy()
    out["sex"] = out["RIAGENDR"].map({1: "Male", 2: "Female"})
    out["age_years"] = out["RIDAGEYR"]
    out["race_eth"] = out["RIDRETH3"].map(RACE_LABELS)
    return out
