from __future__ import annotations

from pathlib import Path
import polars as pl

from .._configs import Paths
from .._utilis import ensure_dir
from ..term_structure import TermStructureBuilder
from ..weights import ConstantMaturityWeighter
from ..synthetic import SyntheticPriceBuilder


def main() -> None:
    paths = Paths()
    ensure_dir(paths.out_term_with_weights.parent)

    term_df = TermStructureBuilder(paths.data_root, paths.expiries_txt).build()
    term_with_weights = ConstantMaturityWeighter().add_weights(
        term_df, [30, 60, 90, 120, 150, 180]
    )
    term_with_weights.write_parquet(paths.out_term_with_weights)

    synthetics = SyntheticPriceBuilder().build(term_with_weights)
    synthetics.write_parquet(paths.out_synthetics)


if __name__ == "__main__":
    main()
