"""
One switch for where Spark runs:
  SPARK_ENGINE=local       -> local PySpark (development, fast, no quota)
  SPARK_ENGINE=databricks  -> Databricks Free Edition via Databricks Connect (serverless)
"""
from __future__ import annotations

import os


def get_spark():
    engine = os.getenv("SPARK_ENGINE", "local")
    if engine == "databricks":
        from databricks.connect import DatabricksSession  # pip install databricks-connect
        # Reads DATABRICKS_HOST and DATABRICKS_TOKEN from the environment.
        return DatabricksSession.builder.serverless(True).getOrCreate()
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder.master("local[*]")
        .appName("exl-migration-factory")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def engine_name() -> str:
    return "databricks-serverless" if os.getenv("SPARK_ENGINE") == "databricks" else "local-pyspark"
