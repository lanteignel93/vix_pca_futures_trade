from __future__ import annotations

import polars as pl

from vix_pca._configs import Paths
from vix_pca._utilis import ensure_dir
from vix_pca.synthetic import SyntheticPriceBuilder


def main() -> None:
    paths = Paths()
    ensure_dir(paths.out_synthetics.parent)

    term_with_weights = pl.read_parquet(paths.out_term_with_weights)
    synthetics = SyntheticPriceBuilder().build(term_with_weights)

    synthetics.write_parquet(paths.out_synthetics)


if __name__ == "__main__":
    main()
