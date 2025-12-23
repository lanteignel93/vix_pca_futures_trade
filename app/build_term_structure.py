from __future__ import annotations

from pathlib import Path
import polars as pl

from .._configs import Paths
from .._utilis import ensure_dir
from ..term_structure import TermStructureBuilder


def main() -> None:
    paths = Paths()
    ensure_dir(paths.out_term_with_weights.parent)

    builder = TermStructureBuilder(
        data_root=paths.data_root, expiries_txt=paths.expiries_txt
    )
    term_df = builder.build()

    # stage1 output optional (raw term without weights)
    term_df.write_parquet(paths.out_term_with_weights.with_name("term_raw.parquet"))


if __name__ == "__main__":
    main()
