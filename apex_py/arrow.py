"""
APEX-DB Arrow Integration — zero-copy data exchange via Apache Arrow.
"""
from __future__ import annotations

from typing import Optional, List, Any

try:
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


# ============================================================================
# APEX-DB ↔ Arrow Schema Mapping
# ============================================================================

# Maps APEX-DB SQL types to Arrow types
APEX_TO_ARROW = {
    "BOOLEAN":   lambda: pa.bool_()       if HAS_PYARROW else None,
    "TINYINT":   lambda: pa.int8()        if HAS_PYARROW else None,
    "SMALLINT":  lambda: pa.int16()       if HAS_PYARROW else None,
    "INTEGER":   lambda: pa.int32()       if HAS_PYARROW else None,
    "BIGINT":    lambda: pa.int64()       if HAS_PYARROW else None,
    "REAL":      lambda: pa.float32()     if HAS_PYARROW else None,
    "DOUBLE":    lambda: pa.float64()     if HAS_PYARROW else None,
    "VARCHAR":   lambda: pa.large_utf8()  if HAS_PYARROW else None,
    "TIMESTAMP": lambda: pa.timestamp("ns", tz="UTC") if HAS_PYARROW else None,
    "DATE":      lambda: pa.date32()      if HAS_PYARROW else None,
}


def apex_type_to_arrow(apex_type: str) -> Any:
    """Convert APEX-DB type string to pyarrow type."""
    if not HAS_PYARROW:
        raise ImportError("pyarrow is required: pip install pyarrow")
    factory = APEX_TO_ARROW.get(apex_type.upper())
    return factory() if factory else pa.float64()


# ============================================================================
# ArrowSession
# ============================================================================

class ArrowSession:
    """
    APEX-DB session with Apache Arrow integration.

    Provides zero-copy data exchange between APEX-DB and:
    - Apache Arrow (RecordBatch, Table)
    - Pandas (via Arrow)
    - Polars (native Arrow)
    - DuckDB (Arrow Table)
    - Ray Dataset (Arrow)

    Example
    -------
    >>> import pyarrow as pa
    >>> pipeline = apex.Pipeline()
    >>> pipeline.start()
    >>> sess = ArrowSession(pipeline)
    >>>
    >>> # Ingest from Arrow Table
    >>> table = pa.table({"sym": [1, 2], "price": [150.0, 200.0]})
    >>> sess.ingest_arrow(table)
    2
    >>>
    >>> # Export as Arrow Table
    >>> table = sess.to_arrow(symbol=1)
    >>> table.schema
    """

    def __init__(self, pipeline: Any):
        if not HAS_PYARROW:
            raise ImportError("pyarrow is required: pip install pyarrow")
        self.pipeline = pipeline

    # ------------------------------------------------------------------
    # Ingest
    # ------------------------------------------------------------------

    def ingest_arrow(
        self,
        table: "pa.Table",
        batch_size: int = 100_000,
    ) -> int:
        """
        Ingest an Arrow Table into APEX-DB.

        Parameters
        ----------
        table : pa.Table
        batch_size : int

        Returns
        -------
        int : rows ingested
        """
        ingested = 0
        col_names = table.schema.names

        for batch in table.to_batches(max_chunksize=batch_size):
            for i in range(batch.num_rows):
                kwargs = {}
                for col_name in col_names:
                    col = batch.column(col_name)
                    val = col[i].as_py()
                    if val is not None:
                        kwargs[col_name] = val

                self.pipeline.ingest(**kwargs)
                ingested += 1

            self.pipeline.drain()

        return ingested

    def ingest_record_batch(self, batch: "pa.RecordBatch") -> int:
        """Ingest a single Arrow RecordBatch."""
        ingested = 0
        col_names = batch.schema.names

        for i in range(batch.num_rows):
            kwargs = {}
            for col_name in col_names:
                col = batch.column(col_name)
                val = col[i].as_py()
                if val is not None:
                    kwargs[col_name] = val
            self.pipeline.ingest(**kwargs)
            ingested += 1

        self.pipeline.drain()
        return ingested

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def to_arrow(
        self,
        symbol: int,
        columns: Optional[List[str]] = None,
        schema: Optional["pa.Schema"] = None,
    ) -> "pa.Table":
        """
        Export APEX-DB data as an Arrow Table.

        Parameters
        ----------
        symbol : int
        columns : list of str, optional
        schema : pa.Schema, optional
            Explicit schema. If None, inferred from numpy arrays.

        Returns
        -------
        pa.Table
        """
        target_cols = columns or ["price", "volume", "timestamp"]
        arrays = {}

        for col_name in target_cols:
            try:
                arr = self.pipeline.get_column(symbol=symbol, name=col_name)
                if arr is not None:
                    if col_name == "timestamp":
                        arrays[col_name] = pa.array(arr,
                                                     type=pa.timestamp("ns", tz="UTC"))
                    else:
                        arrays[col_name] = pa.array(arr)
            except Exception:
                pass

        if not arrays:
            return pa.table({})

        if schema:
            return pa.table(arrays, schema=schema)
        return pa.table(arrays)

    def to_record_batch_reader(
        self,
        symbol: int,
        chunk_size: int = 100_000,
        columns: Optional[List[str]] = None,
    ) -> "pa.RecordBatchReader":
        """
        Export as a RecordBatchReader for streaming consumption.

        Useful for feeding into DuckDB, Ray, or Spark.
        """
        table = self.to_arrow(symbol=symbol, columns=columns)
        return pa.RecordBatchReader.from_batches(
            table.schema,
            table.to_batches(max_chunksize=chunk_size)
        )

    # ------------------------------------------------------------------
    # DuckDB integration
    # ------------------------------------------------------------------

    def to_duckdb(
        self,
        symbol: int,
        table_name: str = "apex_data",
        conn: Any = None,
    ) -> Any:
        """
        Register APEX-DB data as a DuckDB table.

        Parameters
        ----------
        symbol : int
        table_name : str
            DuckDB table/view name.
        conn : duckdb.DuckDBPyConnection, optional
            Existing connection. Creates in-memory connection if None.

        Returns
        -------
        duckdb.DuckDBPyConnection

        Example
        -------
        >>> conn = sess.to_duckdb(symbol=1, table_name="trades")
        >>> conn.execute("SELECT sym, count(*) FROM trades GROUP BY sym").df()
        """
        try:
            import duckdb
        except ImportError:
            raise ImportError("duckdb is required: pip install duckdb")

        arrow_table = self.to_arrow(symbol=symbol)
        if conn is None:
            conn = duckdb.connect()

        # Register Arrow table directly (zero-copy)
        conn.register(table_name, arrow_table)
        return conn

    # ------------------------------------------------------------------
    # Polars zero-copy
    # ------------------------------------------------------------------

    def to_polars_zero_copy(
        self,
        symbol: int,
        columns: Optional[List[str]] = None,
    ) -> Any:
        """
        Export to polars DataFrame via Arrow (true zero-copy).

        Both polars and this function use Arrow internally,
        so no data is copied.
        """
        try:
            import polars as pl
        except ImportError:
            raise ImportError("polars is required: pip install polars")

        arrow_table = self.to_arrow(symbol=symbol, columns=columns)
        return pl.from_arrow(arrow_table)

    # ------------------------------------------------------------------
    # Schema utilities
    # ------------------------------------------------------------------

    def infer_schema(
        self,
        symbol: int,
        sample_rows: int = 100,
    ) -> "pa.Schema":
        """Infer Arrow schema from APEX-DB column data."""
        table = self.to_arrow(symbol=symbol)
        return table.schema

    def apex_schema_to_arrow(
        self,
        apex_schema: List[tuple],
    ) -> "pa.Schema":
        """
        Convert APEX-DB schema description to Arrow schema.

        Parameters
        ----------
        apex_schema : list of (name, apex_type_str)

        Returns
        -------
        pa.Schema
        """
        fields = []
        for name, apex_type in apex_schema:
            fields.append(pa.field(name, apex_type_to_arrow(apex_type)))
        return pa.schema(fields)
