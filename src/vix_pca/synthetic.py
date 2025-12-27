from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from vix_pca._utilis import require_columns


@dataclass
class SyntheticPriceBuilder:
    price_col: str = "close"
    day_col: str = "tradingDay"
    weight_prefix: str = "weight_"
    out_prefix: str = "px_"

    def build(self, term_with_weights: pl.DataFrame) -> pl.DataFrame:
        require_columns(term_with_weights, [self.day_col, self.price_col])

        weight_cols = [c for c in term_with_weights.columns if c.startswith(self.weight_prefix)]
        if not weight_cols:
            raise ValueError(f"No weight columns found with prefix {self.weight_prefix!r}")

        synth_exprs = [
            (pl.col(self.price_col) * pl.col(w))
            .sum()
            .alias(self.out_prefix + w.removeprefix(self.weight_prefix))
            for w in weight_cols
        ]

        return term_with_weights.group_by(self.day_col).agg(synth_exprs).sort(self.day_col)


if __name__ == "__main__":
    import polars as pl
    from vix_futures_trade._configs import Paths
    from vix_futures_trade._utilis import ensure_dir
    from vix_futures_trade.term_structure import TermStructureBuilder
    from vix_futures_trade.weights import ConstantMaturityWeighter

    paths = Paths()
    ensure_dir(paths.out_synthetics.parent)

    # Ensure we have a weighted term_df available.
    # If you want this stage to strictly depend on an existing file,
    # replace this with: term_w = pl.read_parquet(paths.out_term_with_weights)
    term_df = TermStructureBuilder(paths.data_root, paths.expiries_txt).build()
    term_w = ConstantMaturityWeighter().add_weights(term_df, [30, 60, 90, 120, 150, 180])

    builder = SyntheticPriceBuilder()
    synthetics = builder.build(term_w)

    print("synthetics shape:", synthetics.shape)
    print(synthetics.head(10))

    # Sanity checks
    assert synthetics.height > 0, "synthetics is empty"
    assert "tradingDay" in synthetics.columns
    # At least one px_ column exists
    px_cols = [c for c in synthetics.columns if c.startswith("px_")]
    assert len(px_cols) > 0, "No px_ columns produced"
    assert (
        synthetics.select([pl.col(c).is_null().sum().alias(c) for c in px_cols]).row(0) is not None
    )

    synthetics.write_parquet(paths.out_synthetics)
    print("Wrote:", paths.out_synthetics)
