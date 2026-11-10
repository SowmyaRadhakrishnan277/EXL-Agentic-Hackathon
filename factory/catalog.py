"""
Table catalog: one place that maps every legacy table name to its lakehouse table.

    dbo.<t>                         -> bronze.<t>     raw source tables, loaded as-is
    stg.<t>, legacy.stg_<t>,
    legacy.<t>                      -> silver.<t>     cleaned / conformed / intermediate
    rpt.<t>, legacy.rpt_<t>         -> gold.<t>       report-ready

On Databricks a lakehouse table lives in Unity Catalog as <catalog>.<layer>.<t>
(catalog from UC_CATALOG, default "workspace", the Free Edition default).
Locally it is a parquet folder lakehouse/<layer>/<t>/.
"""
from __future__ import annotations

import os
import re

from factory.contracts.models import Layer

LEGACY_SCHEMAS = {"dbo": Layer.BRONZE, "stg": Layer.SILVER, "rpt": Layer.GOLD}


def layer_and_name(legacy_table: str) -> tuple[Layer, str]:
    """'stg.claims' -> (SILVER, 'silver.claims')."""
    schema, name = legacy_table.lower().split(".", 1)
    if schema == "dbo":
        return Layer.BRONZE, f"bronze.{name}"
    if schema == "rpt" or name.startswith("rpt_"):
        return Layer.GOLD, f"gold.{re.sub(r'^rpt_', '', name)}"
    if schema == "stg" or schema == "legacy":
        return Layer.SILVER, f"silver.{re.sub(r'^stg_', '', name)}"
    raise ValueError(f"no lakehouse mapping rule for '{legacy_table}'")


def lakehouse_name(legacy_table: str) -> str:
    return layer_and_name(legacy_table)[1]


def lakehouse_column(legacy_column: str) -> str:
    """'stg.claims.claim_id' -> 'silver.claims.claim_id'."""
    schema, table, col = legacy_column.lower().split(".")
    return f"{lakehouse_name(f'{schema}.{table}')}.{col}"


def uc_name(lakehouse_table: str) -> str:
    """'silver.claims' -> 'workspace.silver.claims' (Unity Catalog three-part name)."""
    return f"{os.getenv('UC_CATALOG') or 'workspace'}.{lakehouse_table}"


def check_no_collisions(legacy_tables: list[str]) -> None:
    seen: dict[str, str] = {}
    for t in legacy_tables:
        target = lakehouse_name(t)
        if target in seen and seen[target] != t:
            raise ValueError(f"{t} and {seen[target]} both map to {target}")
        seen[target] = t
