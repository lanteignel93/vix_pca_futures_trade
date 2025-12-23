from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl
from polars.exceptions import ColumnNotFoundError

from ._configs import TARGET_TIME_CT
from ._utilis import (
    parse_expiry_file,
    contract_key_from_symbol,
    infer_symbol_from_filename,
    require_columns,
)


@dataclass
class TermStructureBuilder:
    data_root: Path
    expiries_txt: Path
    file_glob: str = "*.csv"

    def _pick_close_near_255_ct(self, df: pl.DataFrame) -> pl.DataFrame:
        require_columns(df, ["timestamp", "tradingDay", "close"])

        if df.schema.get("timestamp") != pl.Datetime:
            df = df.with_columns(
                pl.col("timestamp")
                .str.strptime(pl.Datetime, strict=False)
                .alias("timestamp")
            )

        df = df.with_columns(
            pl.col("tradingDay")
            .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            .alias("tradingDay")
        )

        df = df.with_columns(
            pl.datetime(
                pl.col("tradingDay").dt.year(),
                pl.col("tradingDay").dt.month(),
                pl.col("tradingDay").dt.day(),
                pl.lit(TARGET_TIME_CT.hour),
                pl.lit(TARGET_TIME_CT.minute),
                0,
                time_zone="America/Chicago",
            ).alias("target_ts")
        )

        df = df.with_columns(
            pl.col("timestamp").dt.convert_time_zone("America/Chicago").alias("ts_ct")
        )

        df = df.with_columns(
            (pl.col("ts_ct") - pl.col("target_ts")).abs().alias("abs_diff")
        )

        return (
            df.sort(["symbol", "tradingDay", "abs_diff"])
            .group_by(["symbol", "tradingDay"], maintain_order=True)
            .first()
            .select(["symbol", "tradingDay", "ts_ct", "close"])
        )

    def build(self) -> pl.DataFrame:
        expiries = parse_expiry_file(self.expiries_txt)

        csv_paths = sorted(self.data_root.rglob(self.file_glob))
        if not csv_paths:
            raise FileNotFoundError(f"No CSVs found under: {self.data_root}")

        frames: list[pl.DataFrame] = []

        for p in csv_paths:
            try:
                df = pl.read_csv(
                    p,
                    columns=["symbol", "timestamp", "tradingDay", "close"],
                    infer_schema_length=2000,
                )
            except ColumnNotFoundError:
                # Skip unreadable schema files deterministically
                continue

            # if symbol missing or all null: infer from filename
            if "symbol" not in df.columns or df["symbol"].is_null().all():
                inferred = infer_symbol_from_filename(p)
                if inferred is None:
                    raise ValueError(f"Cannot infer symbol from filename: {p.name}")
                df = df.with_columns(pl.lit(inferred).alias("symbol"))

            picked = self._pick_close_near_255_ct(df)

            sym = picked["symbol"].to_list()
            td = picked["tradingDay"].to_list()

            expiry_dates: list[date | None] = []
            ttm_days: list[int | None] = []

            for s, d in zip(sym, td):
                ck = contract_key_from_symbol(s)
                exp = expiries.get(ck)
                expiry_dates.append(exp)
                ttm_days.append((exp - d).days if exp is not None else None)

            picked = picked.with_columns(
                pl.Series("expiry", expiry_dates),
                pl.Series("ttm_days", ttm_days),
            ).filter(pl.col("ttm_days").is_not_null() & (pl.col("ttm_days") > 0))

            frames.append(picked)

        out = pl.concat(frames, how="vertical").unique(
            subset=["symbol", "tradingDay"], keep="first"
        )
        return out.sort(["tradingDay", "ttm_days", "symbol"])


if __name__ == "__main__":
    from pathlib import Path
    import polars as pl

    from vix_futures_trade._configs import Paths
    from vix_futures_trade._utilis import ensure_dir

    paths = Paths()
    ensure_dir(paths.out_term_with_weights.parent)

    builder = TermStructureBuilder(
        data_root=paths.data_root,
        expiries_txt=paths.expiries_txt,
    )

    term_df = builder.build()

    print("term_df shape:", term_df.shape)
    print(term_df.head(10))

    # Basic sanity checks
    assert term_df.height > 0, "term_df is empty"
    assert {"symbol", "tradingDay", "close", "expiry", "ttm_days"}.issubset(
        set(term_df.columns)
    )
    assert (
        term_df.filter(pl.col("ttm_days") <= 0).height == 0
    ), "Found non-positive ttm_days"
    assert (
        term_df.filter(pl.col("close").is_null()).height == 0
    ), "Found null close values"

    # Optional: write a raw output for inspection
    out_path = paths.out_term_with_weights.with_name("term_raw.parquet")
    term_df.write_parquet(out_path)
    print("Wrote:", out_path)
