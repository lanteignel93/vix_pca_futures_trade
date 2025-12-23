from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from pathlib import Path
from zoneinfo import ZoneInfo
import re

CT_TZ = ZoneInfo("America/Chicago")
TARGET_TIME_CT = time(14, 55)  # 2:55pm CT

FUT_MONTHS = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]
CODE_TO_MONTH = {c: i + 1 for i, c in enumerate(FUT_MONTHS)}

CONTRACT_RE = re.compile(r"VI(?P<mcode>[FGHJKMNQUVXZ])(?P<yy>\d{2})$", re.IGNORECASE)
FILENAME_RE = re.compile(r"vi(?P<mcode>[fghjkmnquvxz])(?P<yy>\d{2})", re.IGNORECASE)

PROJECT_ROOT = Path(__file__).resolve().parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
EXPIRIES_TXT = CONFIG_DIR / "expiries.txt"


@dataclass(frozen=True)
class Paths:
    """Centralize IO paths; can be overridden by callers."""
    data_root: Path  # where CSVs live (external path)
    expiries_txt: Path = EXPIRIES_TXT
    out_term_with_weights: Path = DATA_DIR / "term_with_weights.parquet"
    out_synthetics: Path = DATA_DIR / "synthetic_prices.parquet"

