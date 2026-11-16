"""
Two-system validator: compares the same data in two places (a legacy system and a cloud
system) and writes a validation report. Each side can be an Excel / CSV / Parquet extract,
a SQL Server table or query, or a Databricks table or query.

Standalone: nothing in factory/ imports this package. Run it with `python -m validator`.
See README.md in this folder.
"""
from .compare import ColumnRule, ConfigError, Rules, compare

__all__ = ["ColumnRule", "ConfigError", "Rules", "compare"]
