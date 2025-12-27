from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from ._utilis import require_columns


@dataclass
class ConstantMaturityWeighter:
    clamp: bool = True

    def add_weights(self, term_df: pl.DataFrame, tenors: list[int]) -> pl.DataFrame:
        require_columns(term_df, ["tradingDay", "symbol", "ttm_days"])

        out = term_df
        for t in tenors:
            w = self._weights_for_tenor(term_df=out, target_days=float(t))
            out = out.join(w, on=["tradingDay", "symbol", "ttm_days"], how="left")
        return out

    def _weights_for_tenor(self, term_df: pl.DataFrame, target_days: float) -> pl.DataFrame:
        df = term_df.select(["tradingDay", "symbol", "ttm_days"]).sort(
            ["tradingDay", "ttm_days", "symbol"]
        )

        lower = (
            df.filter(pl.col("ttm_days") <= target_days)
            .group_by("tradingDay")
            .agg(pl.col("ttm_days").max().alias("d1"))
        )
        upper = (
            df.filter(pl.col("ttm_days") >= target_days)
            .group_by("tradingDay")
            .agg(pl.col("ttm_days").min().alias("d2"))
        )

        bounds = (
            df.select("tradingDay")
            .unique()
            .join(lower, on="tradingDay", how="left")
            .join(upper, on="tradingDay", how="left")
        )

        if not self.clamp:
            bad = bounds.filter(pl.col("d1").is_null() | pl.col("d2").is_null())
            if bad.height > 0:
                raise ValueError("Target outside available maturities for at least one tradingDay.")

        bounds = (
            bounds.join(
                df.group_by("tradingDay").agg(
                    pl.col("ttm_days").min().alias("min_d"),
                    pl.col("ttm_days").max().alias("max_d"),
                ),
                on="tradingDay",
                how="left",
            )
            .with_columns(
                pl.when(pl.col("d1").is_null())
                .then(pl.col("min_d"))
                .otherwise(pl.col("d1"))
                .alias("d1"),
                pl.when(pl.col("d2").is_null())
                .then(pl.col("max_d"))
                .otherwise(pl.col("d2"))
                .alias("d2"),
            )
            .drop(["min_d", "max_d"])
        )

        d1_sym = (
            df.join(bounds.select(["tradingDay", "d1"]), on="tradingDay", how="inner")
            .filter(pl.col("ttm_days") == pl.col("d1"))
            .group_by("tradingDay")
            .agg(pl.col("symbol").first().alias("sym1"))
        )
        d2_sym = (
            df.join(bounds.select(["tradingDay", "d2"]), on="tradingDay", how="inner")
            .filter(pl.col("ttm_days") == pl.col("d2"))
            .group_by("tradingDay")
            .agg(pl.col("symbol").first().alias("sym2"))
        )

        bounds = bounds.join(d1_sym, on="tradingDay", how="left").join(
            d2_sym, on="tradingDay", how="left"
        )

        weight_col = f"weight_{int(target_days)}d"

        out = (
            df.join(bounds, on="tradingDay", how="left")
            .with_columns(
                pl.when(pl.col("d1") == pl.col("d2"))
                .then(pl.when(pl.col("symbol") == pl.col("sym1")).then(1.0).otherwise(0.0))
                .otherwise(
                    pl.when(pl.col("symbol") == pl.col("sym1"))
                    .then((pl.col("d2") - pl.lit(target_days)) / (pl.col("d2") - pl.col("d1")))
                    .when(pl.col("symbol") == pl.col("sym2"))
                    .then((pl.lit(target_days) - pl.col("d1")) / (pl.col("d2") - pl.col("d1")))
                    .otherwise(0.0)
                )
                .alias(weight_col)
            )
            .select(["tradingDay", "symbol", "ttm_days", weight_col])
        )
        return out


if __name__ == "__main__":
    import polars as pl
    from vix_futures_trade._configs import Paths
    from vix_futures_trade._utilis import ensure_dir
    from vix_futures_trade.term_structure import TermStructureBuilder

    paths = Paths()
    ensure_dir(paths.out_term_with_weights.parent)

    # Build term_df first (weights stage input)
    term_df = TermStructureBuilder(
        data_root=paths.data_root,
        expiries_txt=paths.expiries_txt,
    ).build()

    tenors = [30, 60, 90, 120, 150, 180]
    weighter = ConstantMaturityWeighter(clamp=True)

    term_w = weighter.add_weights(term_df, tenors)

    print("term_w shape:", term_w.shape)
    print(
        term_w.select(
            ["tradingDay", "symbol", "ttm_days", "close"] + [f"weight_{t}d" for t in tenors]
        ).head(10)
    )

    # Sanity checks: weight columns exist
    for t in tenors:
        col = f"weight_{t}d"
        assert col in term_w.columns, f"Missing weight column: {col}"

    # Optional: verify weights sum to ~1 per tradingDay for each tenor
    # (because only two contracts should have non-zero weights)
    sums = term_w.group_by("tradingDay").agg(
        [pl.col(f"weight_{t}d").sum().alias(f"sum_{t}d") for t in tenors]
    )

    # allow small numeric tolerance
    tol = 1e-6
    for t in tenors:
        bad = sums.filter((pl.col(f"sum_{t}d") - 1.0).abs() > tol)
        assert bad.height == 0, f"Weight sums not ~1 for tenor {t}d; sample:\n{bad.head(5)}"

    # Write the weighted term structure (this is your requested stage-2 artifact)
    term_w.write_parquet(paths.out_term_with_weights)
    print("Wrote:", paths.out_term_with_weights)
