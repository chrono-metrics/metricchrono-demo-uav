"""I/O helpers for large Parquet datasets."""

from __future__ import annotations

from pathlib import Path
from typing import Union

import pandas as pd
import pyarrow.parquet as pq


PathLike = Union[str, Path]


def read_parquet(path: PathLike) -> pd.DataFrame:
    """Read a Parquet file via PyArrow, avoiding pandas' dataset wrapper quirks."""

    table = pq.read_table(path)
    return table.to_pandas()
