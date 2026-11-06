"""
Design agent: DiscoveryResult in, DesignSpec (layer + data contracts) out.

Strategy ("rules decide structure, LLM fills details, guardrails check it"):
  Layer / table name -> deterministic rule from the output table name.
  Columns            -> taken from the discovered lineage (ground truth for which
                        columns exist). Baseline dtype comes from the source DDL for
                        direct copies; the LLM refines dtype / nullability / rules.
  Primary key        -> LLM proposal, validated as a subset of the columns; falls back
                        to the non-aggregate columns (the GROUP BY keys).
Anything the LLM invents (extra columns, bad keys, unparseable dtypes) is dropped
and logged in `DesignResult.notes`.

    python -m factory.agents.design runs/<run_id>      # reads discovery/, writes design/
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from factory.config import ROOT, load_env
from factory.contracts.models import (ColumnContract, DataContract, DesignSpec,
                                      DiscoveryResult, Layer)
from factory.llm.base import LLM, get_llm

DDL = ROOT / "data" / "ddl" / "create_tables.sql"
AGG = re.compile(r"\b(SUM|COUNT|AVG|MIN|MAX)\s*\(", re.I)
DTYPE = re.compile(r"^(string|boolean|int|bigint|double|date|timestamp|decimal\(\d{1,2},\s*\d{1,2}\))$")


class DesignResult(BaseModel):
    spec: DesignSpec
    notes: list[str] = []


# ============================================================== rules

def layer_and_name(table: str) -> tuple[Layer, str]:
    """rpt.x / legacy.rpt_x -> gold.x;  stg.x / legacy.stg_x / legacy.x -> silver.x."""
    _, name = table.lower().split(".", 1)
    if table.lower().startswith("rpt.") or name.startswith("rpt_"):
        return Layer.GOLD, f"gold.{re.sub(r'^rpt_', '', name)}"
    return Layer.SILVER, f"silver.{re.sub(r'^stg_', '', name)}"


_SQL_TO_SPARK = {"varchar": "string", "char": "string", "date": "date", "datetime": "timestamp",
                 "int": "int", "bigint": "bigint", "float": "double", "bit": "boolean"}


def source_types() -> dict[str, tuple[str, bool]]:
    """'dbo.table.column' -> (spark dtype, nullable), read from the legacy DDL."""
    out: dict[str, tuple[str, bool]] = {}
    for m in re.finditer(r"CREATE TABLE\s+(\w+)\.(\w+)\s*\((.*?)\n\);", DDL.read_text(), re.S | re.I):
        schema, table, body = m.groups()
        for line in body.split("\n"):
            c = re.match(r"\s*(\w+)\s+(\w+)(?:\((\d+)(?:,\s*(\d+))?\))?\s*(NOT NULL|NULL)?", line)
            if not c:
                continue
            col, typ, p, s, nn = c.groups()
            typ = typ.lower()
            dtype = f"decimal({p},{s})" if typ == "decimal" else _SQL_TO_SPARK.get(typ, "string")
            out[f"{schema}.{table}.{col}".lower()] = (dtype, nn is None or nn.upper() != "NOT NULL")
    return out


def baseline_columns(r: DiscoveryResult, table: str, types: dict) -> list[ColumnContract]:
    cols = []
    for lin in (c for c in r.column_lineage if c.target_table == table):
        dtype, nullable = "string", True
        src = lin.source_columns[0] if len(lin.source_columns) == 1 else None
        # staging tables mirror their dbo source, so stg.x.col falls back to dbo.x.col
        src = src if src in types else (re.sub(r"^\w+\.", "dbo.", src) if src else None)
        if lin.transformation.upper().startswith("COUNT"):
            dtype, nullable = "bigint", False
        elif src in types and not re.search(r"\b(CASE|AVG|COUNT)\b|[+*/]", lin.transformation, re.I):
            dtype, nullable = types[src]            # copy, SUM, MIN, MAX keep the source type
        cols.append(ColumnContract(name=lin.target_column, dtype=dtype, nullable=nullable))
    return cols


def fallback_key(r: DiscoveryResult, table: str, cols: list[ColumnContract]) -> list[str]:
    non_agg = [c.target_column for c in r.column_lineage
               if c.target_table == table and not AGG.search(c.transformation)]
    return non_agg or [c.name for c in cols]


# ============================================================== LLM

SYSTEM = """You are a data architect designing a lakehouse data contract for a table
migrated from SQL Server T-SQL / SAS to Spark. Be precise and conservative.
Only use the columns listed. Reply with JSON only."""

PROMPT = """Legacy job {job_id}: {summary}

Target table: {table}   (layer: {layer})

Columns, with lineage and a baseline dtype taken from the source schema:
{columns}

Risks noted during discovery:
{risks}

Return JSON:
{{"primary_key": ["<columns that uniquely identify a row; for aggregates the GROUP BY columns>"],
  "columns": [
    {{"name": "<column>",
      "dtype": "<one of: string, boolean, int, bigint, double, date, timestamp, decimal(p,s)>",
      "nullable": <true|false>,
      "rule": "<optional check, e.g. '>= 0' or \\"in ('0-30','31-90')\\"; null if none>"}}
  ]}}
Money is decimal, never double. A column wrapped in COALESCE or filtered NOT NULL is not nullable.
Counts are bigint and not nullable."""


def design_table(r: DiscoveryResult, table: str, llm: LLM, types: dict,
                 notes: list[str]) -> tuple[DataContract, str]:
    layer, new_name = layer_and_name(table)
    base = baseline_columns(r, table, types)
    by_name = {c.name: c for c in base}
    key = fallback_key(r, table, base)

    if llm.available:
        listing = "\n".join(
            f"- {c.target_column}: {c.transformation}  <- {', '.join(c.source_columns) or '(none)'}"
            f"  [baseline {by_name[c.target_column].dtype}]"
            for c in r.column_lineage if c.target_table == table)
        reply = llm.complete_json(SYSTEM, PROMPT.format(
            job_id=r.job_id, summary=r.summary or "(no summary)", table=new_name, layer=layer.value,
            columns=listing, risks="\n".join(f"- {x}" for x in r.risk_notes) or "- none"))
        for c in reply.get("columns", []):
            name = str(c.get("name", "")).lower()
            if name not in by_name:
                notes.append(f"guardrail: dropped invented column '{name}' in {new_name}")
                continue
            dtype = str(c.get("dtype", "")).lower().replace(" ", "")
            if not DTYPE.match(dtype):
                notes.append(f"guardrail: bad dtype '{c.get('dtype')}' for {new_name}.{name}, kept {by_name[name].dtype}")
                dtype = by_name[name].dtype
            by_name[name] = ColumnContract(name=name, dtype=dtype, nullable=bool(c.get("nullable", True)),
                                           rule=c.get("rule") or None)
        proposed = [str(k).lower() for k in reply.get("primary_key", [])]
        if proposed and all(k in by_name for k in proposed):
            key = proposed
        else:
            notes.append(f"guardrail: primary key {proposed} invalid for {new_name}, "
                         f"using GROUP BY columns {key}")
    else:
        notes.append("no LLM configured: contract built from parser baseline only")

    for k in key:                                   # a key column can never be null
        c = by_name[k]
        by_name[k] = c.model_copy(update={"nullable": False})
    return DataContract(table=new_name, primary_key=key, columns=list(by_name.values())), new_name


# ============================================================== main

def design(r: DiscoveryResult, llm: Optional[LLM] = None) -> DesignResult:
    llm = llm or get_llm()
    types, notes = source_types(), []
    contracts, names, layers = [], [], set()
    for table in r.output_tables:
        contract, name = design_table(r, table, llm, types, notes)
        contracts.append(contract)
        names.append(name)
        layers.add(layer_and_name(table)[0])
    layer = Layer.GOLD if Layer.GOLD in layers else Layer.SILVER
    return DesignResult(spec=DesignSpec(job_id=r.job_id, layer=layer, target_tables=names,
                                        contracts=contracts), notes=notes)


def main() -> None:
    load_env()
    run_dir = ROOT / sys.argv[1]
    llm = get_llm()
    out = run_dir / "design"
    out.mkdir(exist_ok=True)
    for f in sorted((run_dir / "discovery").glob("J*.json")):
        r = DiscoveryResult.model_validate_json(f.read_text())
        res = design(r, llm)
        (out / f.name).write_text(res.model_dump_json(indent=2))
        print(f"  {r.job_id:34s} {res.spec.layer.value:7s} {', '.join(res.spec.target_tables)}"
              f"  ({len(res.notes)} notes)")


if __name__ == "__main__":
    sys.exit(main())
