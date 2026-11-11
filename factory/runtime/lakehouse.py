"""
Read and write lakehouse tables by their lakehouse name ("bronze.claims", "gold.loss_ratio").

  SPARK_ENGINE=databricks -> Delta tables in Unity Catalog: <UC_CATALOG>.<layer>.<table>
  otherwise (local)       -> parquet folders: <LAKEHOUSE_DIR or ./lakehouse>/<layer>/<table>/

Generated jobs keep their `run(spark, tables)` signature with LEGACY names as keys
("dbo.claims", "stg.policies"); `load_inputs` resolves those through factory.catalog,
so the same generated code runs against the lakehouse without changes.
"""
from __future__ import annotations

import os
from pathlib import Path

from factory import catalog
from factory.config import ROOT

LAYERS = ("bronze", "silver", "gold")


def is_databricks() -> bool:
    return os.getenv("SPARK_ENGINE", "local") == "databricks"


def local_root() -> Path:
    return Path(os.getenv("LAKEHOUSE_DIR") or ROOT / "lakehouse")


def _local_path(lakehouse_table: str) -> Path:
    layer, name = lakehouse_table.split(".", 1)
    return local_root() / layer / name


def ensure_schemas(spark) -> None:
    if is_databricks():
        cat = os.getenv("UC_CATALOG") or "workspace"
        for layer in LAYERS:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cat}.{layer}")


def read_table(spark, lakehouse_table: str):
    if is_databricks():
        return spark.table(catalog.uc_name(lakehouse_table))
    return spark.read.parquet(str(_local_path(lakehouse_table)))


def write_table(df, lakehouse_table: str) -> None:
    if is_databricks():
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
           .saveAsTable(catalog.uc_name(lakehouse_table)))
    else:
        df.write.mode("overwrite").parquet(str(_local_path(lakehouse_table)))


def load_inputs(spark, legacy_tables: list[str], drop_audit: bool = True) -> dict:
    """{'dbo.claims': DataFrame, ...} read from the mapped lakehouse tables.

    Bronze audit columns (_ingested_at, _source_file) are dropped by default so a
    migrated job sees exactly the columns the legacy job saw."""
    out = {}
    for t in legacy_tables:
        df = read_table(spark, catalog.lakehouse_name(t))
        if drop_audit:
            df = df.drop(*[c for c in df.columns if c.startswith("_")])
        out[t.lower()] = df
    return out
