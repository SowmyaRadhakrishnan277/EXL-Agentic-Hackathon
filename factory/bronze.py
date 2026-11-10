"""
Bronze layer: load the 7 raw source extracts into bronze.<table>, as-is.

  * Types come from the legacy DDL (data/ddl/create_tables.sql), so bronze matches
    what SQL Server holds: same columns, same order, same decimals and dates.
  * No cleaning: messy text, NULLs and duplicate rows are kept on purpose; cleaning is
    the job of the migrated silver jobs, exactly as in the legacy estate.
  * Two audit columns are added: _ingested_at (load timestamp) and _source_file.
  * FAILFAST: a value that does not fit its DDL type stops the load instead of
    silently becoming NULL.

    python -m factory.bronze                     # local: data/raw -> lakehouse/bronze/
    SPARK_ENGINE=databricks RAW_PATH=/Volumes/workspace/bronze/raw python -m factory.bronze
"""
from __future__ import annotations

import os
import sys
import time

from factory.agents.design import source_types
from factory.config import ROOT, load_env
from factory.runtime import lakehouse

RAW_TABLES = ["products", "brokers", "customers", "policies", "premiums", "claims", "claim_payments"]


def bronze_ddl(table: str) -> str:
    """Spark DDL schema string for a raw table, in DDL column order."""
    cols = [(full.split(".")[2], dtype) for full, (dtype, _) in source_types().items()
            if full.startswith(f"dbo.{table}.")]
    if not cols:
        raise ValueError(f"dbo.{table} not found in the legacy DDL")
    return ", ".join(f"{c} {t.upper()}" for c, t in cols)


def ingest(spark, raw_path: str, tables: list[str] = RAW_TABLES) -> dict[str, int]:
    from pyspark.sql import functions as F
    lakehouse.ensure_schemas(spark)
    counts = {}
    for t in tables:
        t0 = time.time()
        df = (spark.read.option("header", True).option("mode", "FAILFAST")
              .schema(bronze_ddl(t)).csv(f"{raw_path.rstrip('/')}/{t}.csv")
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.lit(f"{t}.csv")))
        lakehouse.write_table(df, f"bronze.{t}")
        counts[t] = lakehouse.read_table(spark, f"bronze.{t}").count()
        print(f"  bronze.{t:15s} {counts[t]:>9,} rows  {time.time() - t0:5.1f}s")
    return counts


def main() -> None:
    load_env()
    from factory.runtime.spark import get_spark
    raw = os.getenv("RAW_PATH") or str(ROOT / "data" / "raw")
    print(f"Bronze load from {raw} ({'Unity Catalog' if lakehouse.is_databricks() else lakehouse.local_root()})")
    ingest(get_spark(), raw)


if __name__ == "__main__":
    sys.exit(main())
