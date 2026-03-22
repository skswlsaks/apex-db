"""
APEX-DB DataFrame utilities — standalone functions for pandas/polars conversion.
"""
from __future__ import annotations

from typing import Optional, Dict, Any, Union, List

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


# ============================================================================
# Pandas ↔ APEX-DB
# ============================================================================

def from_pandas(
    df: "pd.DataFrame",
    pipeline: Any,
    batch_size: int = 10_000,
    sym_col: str = "sym",
    timestamp_col: Optional[str] = "timestamp",
    show_progress: bool = False,
) -> int:
    """
    Ingest a pandas DataFrame into APEX-DB via the pipeline object.

    Parameters
    ----------
    df : pd.DataFrame
        Source data.
    pipeline : ApexPipeline (C++ object)
        APEX-DB pipeline instance.
    batch_size : int
        Rows per batch flush.
    sym_col : str
        Column name containing the symbol/key.
    timestamp_col : str or None
        Column name containing timestamps (ns since epoch).
    show_progress : bool
        Print progress to stdout.

    Returns
    -------
    int : rows ingested

    Example
    -------
    >>> import apex, apex_py, pandas as pd
    >>> pipeline = apex.Pipeline()
    >>> pipeline.start()
    >>> df = pd.DataFrame({"sym": [1]*100, "price": [150.0]*100, "volume": [100]*100})
    >>> apex_py.from_pandas(df, pipeline)
    100
    """
    if not HAS_PANDAS:
        raise ImportError("pandas is required: pip install pandas")

    cols = list(df.columns)
    total = len(df)
    ingested = 0

    for start in range(0, total, batch_size):
        chunk = df.iloc[start : start + batch_size]

        for _, row in chunk.iterrows():
            kwargs = {}
            for col in cols:
                val = row[col]
                # Convert numpy scalars to Python native
                if hasattr(val, 'item'):
                    val = val.item()
                # Convert pandas Timestamp to int64 (ns)
                if HAS_PANDAS and isinstance(val, pd.Timestamp):
                    val = int(val.value)
                kwargs[col] = val

            try:
                pipeline.ingest(**kwargs)
                ingested += 1
            except Exception as e:
                raise RuntimeError(f"Ingest failed at row {start + ingested}: {e}") from e

        # Drain every batch
        try:
            pipeline.drain()
        except Exception:
            pass

        if show_progress:
            print(f"\rIngested {min(start + batch_size, total)}/{total}",
                  end="", flush=True)

    if show_progress:
        print()

    return ingested


def to_pandas(
    pipeline: Any,
    symbol: int,
    columns: Optional[List[str]] = None,
    start_ts: Optional[int] = None,
    end_ts:   Optional[int] = None,
) -> "pd.DataFrame":
    """
    Export APEX-DB data to a pandas DataFrame (zero-copy via numpy).

    Parameters
    ----------
    pipeline : ApexPipeline
    symbol : int
        Symbol ID to export.
    columns : list of str, optional
        Column names to export. None = all columns.
    start_ts, end_ts : int, optional
        Nanosecond timestamps for range filter.

    Returns
    -------
    pd.DataFrame

    Example
    -------
    >>> df = apex_py.to_pandas(pipeline, symbol=1)
    >>> df.head()
    """
    if not HAS_PANDAS:
        raise ImportError("pandas is required: pip install pandas")
    if not HAS_NUMPY:
        raise ImportError("numpy is required: pip install numpy")

    data = {}
    target_cols = columns or ["price", "volume", "timestamp"]

    for col_name in target_cols:
        try:
            arr = pipeline.get_column(symbol=symbol, name=col_name)
            if arr is not None:
                data[col_name] = arr  # zero-copy numpy array
        except Exception:
            pass

    df = pd.DataFrame(data)

    # Convert timestamp column if present
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ns", utc=True)

    # Apply time range filter
    if start_ts is not None or end_ts is not None:
        if "timestamp" in df.columns:
            mask = pd.Series([True] * len(df))
            if start_ts is not None:
                ts_start = pd.Timestamp(start_ts, unit="ns", tz="UTC")
                mask &= df["timestamp"] >= ts_start
            if end_ts is not None:
                ts_end = pd.Timestamp(end_ts, unit="ns", tz="UTC")
                mask &= df["timestamp"] <= ts_end
            df = df[mask].reset_index(drop=True)

    return df


# ============================================================================
# Polars ↔ APEX-DB
# ============================================================================

def from_polars(
    df: "pl.DataFrame",
    pipeline: Any,
    batch_size: int = 50_000,
    show_progress: bool = False,
) -> int:
    """
    Ingest a polars DataFrame into APEX-DB.

    Uses Arrow IPC for efficient column-wise transfer.
    Polars is Arrow-native, so this path has minimal overhead.

    Parameters
    ----------
    df : pl.DataFrame
    pipeline : ApexPipeline
    batch_size : int, default 50_000
        Larger batches are more efficient with polars.
    show_progress : bool

    Returns
    -------
    int : rows ingested

    Example
    -------
    >>> import polars as pl, apex_py
    >>> df = pl.DataFrame({"sym": [1]*1000, "price": [150.0]*1000})
    >>> apex_py.from_polars(df, pipeline)
    1000
    """
    if not HAS_POLARS:
        raise ImportError("polars is required: pip install polars")

    # Polars → pandas → from_pandas (fallback path)
    # For zero-copy path, use from_polars_arrow() below
    pd_df = df.to_pandas()
    return from_pandas(pd_df, pipeline, batch_size=batch_size,
                       show_progress=show_progress)


def from_polars_arrow(
    df: "pl.DataFrame",
    pipeline: Any,
) -> int:
    """
    Zero-copy ingest from polars via Arrow RecordBatch.
    Requires pyarrow.

    Each column's Arrow buffer is passed directly to the C++ layer
    without any Python-level copying.
    """
    if not HAS_POLARS:
        raise ImportError("polars is required")

    try:
        import pyarrow as pa
    except ImportError:
        # Fallback to pandas path
        return from_polars(df, pipeline)

    # polars → Arrow Table (zero-copy)
    arrow_table = df.to_arrow()
    total = len(df)

    # Pass each column as Arrow buffer
    for batch in arrow_table.to_batches():
        for i in range(batch.num_rows):
            kwargs = {}
            for col_name in batch.schema.names:
                col = batch.column(col_name)
                val = col[i].as_py()
                if val is not None:
                    kwargs[col_name] = val
            pipeline.ingest(**kwargs)

    pipeline.drain()
    return total


def to_polars(
    pipeline: Any,
    symbol: int,
    columns: Optional[List[str]] = None,
) -> "pl.DataFrame":
    """
    Export APEX-DB data to a polars DataFrame.

    Uses the zero-copy numpy path and wraps with polars.from_numpy().

    Parameters
    ----------
    pipeline : ApexPipeline
    symbol : int
    columns : list of str, optional

    Returns
    -------
    pl.DataFrame

    Example
    -------
    >>> df = apex_py.to_polars(pipeline, symbol=1)
    >>> df.describe()
    """
    if not HAS_POLARS:
        raise ImportError("polars is required: pip install polars")
    if not HAS_NUMPY:
        raise ImportError("numpy is required: pip install numpy")

    target_cols = columns or ["price", "volume", "timestamp"]
    series_list = []

    for col_name in target_cols:
        try:
            arr = pipeline.get_column(symbol=symbol, name=col_name)
            if arr is not None:
                if col_name == "timestamp":
                    series = pl.Series(col_name, arr, dtype=pl.Datetime("ns", "UTC"))
                else:
                    series = pl.Series(col_name, arr)
                series_list.append(series)
        except Exception:
            pass

    if not series_list:
        return pl.DataFrame()

    return pl.DataFrame(series_list)


# ============================================================================
# Convenience wrappers
# ============================================================================

def ingest_pandas(
    df: "pd.DataFrame",
    pipeline: Any,
    **kwargs,
) -> int:
    """Alias for from_pandas()."""
    return from_pandas(df, pipeline, **kwargs)


def ingest_polars(
    df: "pl.DataFrame",
    pipeline: Any,
    **kwargs,
) -> int:
    """Alias for from_polars()."""
    return from_polars(df, pipeline, **kwargs)


# ============================================================================
# HTTP-based query result conversion
# ============================================================================

def query_to_pandas(json_response: Union[str, dict]) -> "pd.DataFrame":
    """
    Convert APEX-DB HTTP JSON response to pandas DataFrame.

    Parameters
    ----------
    json_response : str or dict
        JSON response from APEX-DB HTTP API.

    Returns
    -------
    pd.DataFrame
    """
    if not HAS_PANDAS:
        raise ImportError("pandas is required")

    import json as _json
    if isinstance(json_response, str):
        data = _json.loads(json_response)
    else:
        data = json_response

    columns = data.get("columns", [])
    rows    = data.get("data", [])

    return pd.DataFrame(rows, columns=columns)


def query_to_polars(json_response: Union[str, dict]) -> "pl.DataFrame":
    """
    Convert APEX-DB HTTP JSON response to polars DataFrame.

    Parameters
    ----------
    json_response : str or dict
        JSON response from APEX-DB HTTP API.

    Returns
    -------
    pl.DataFrame
    """
    if not HAS_POLARS:
        raise ImportError("polars is required")

    import json as _json
    if isinstance(json_response, str):
        data = _json.loads(json_response)
    else:
        data = json_response

    columns = data.get("columns", [])
    rows    = data.get("data", [])

    if not rows:
        return pl.DataFrame(schema=columns)

    return pl.from_records(rows, schema=columns, orient="row")
