# vix_futures_trade/_utilis.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import polars as pl

from ._configs import CODE_TO_MONTH, CONTRACT_RE, FILENAME_RE


@dataclass(frozen=True)
class ContractKey:
    year: int
    month: int


def parse_expiry_file(expiry_txt_path: Path) -> dict[ContractKey, date]:
    expiries: dict[ContractKey, date] = {}
    for raw in expiry_txt_path.read_text().splitlines():
        raw = raw.strip()
        if not raw:
            continue
        dt = datetime.strptime(raw, "%d %B %Y").date()
        expiries[ContractKey(dt.year, dt.month)] = dt
    return expiries


def contract_key_from_symbol(symbol: str) -> ContractKey:
    m = CONTRACT_RE.search(symbol.strip().upper())
    if not m:
        raise ValueError(f"Unrecognized contract symbol format: {symbol!r}")
    mcode = m.group("mcode").upper()
    yy = int(m.group("yy"))
    year = 2000 + yy if yy <= 79 else 1900 + yy
    month = CODE_TO_MONTH[mcode]
    return ContractKey(year, month)


def infer_symbol_from_filename(path: Path) -> str | None:
    m = FILENAME_RE.search(path.name)
    if not m:
        return None
    mcode = m.group("mcode").upper()
    yy = m.group("yy")
    return f"VI{mcode}{yy}"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def require_columns(df: pl.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
