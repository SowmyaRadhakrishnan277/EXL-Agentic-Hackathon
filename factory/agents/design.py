"""
Design agent: DiscoveryResult in, DesignSpec (layer + data contracts) out.

Strategy ("rules decide structure, LLM fills details, guardrails check it"):
  Layer / table name -> deterministic rule from factory.catalog (bronze/silver/gold).
  Columns            -> taken from the discovered lineage (ground truth for which
                        columns exist). Baseline dtype is inferred from the legacy
                        expression and the source column types (source DDL for raw
                        tables, upstream contracts for staging tables); the LLM refines
                        dtype / nullability / rules.
  Primary key        -> LLM proposal, validated as a subset of the columns. Fallback:
                        aggregates -> the GROUP BY columns; row-level tables -> the
                        identifier columns copied from an input table's key, plus any
                        generated columns (e.g. a year from a VALUES list).
  SAS jobs with no lineage (no LLM) -> columns read from the golden output header,
                        if the SAS job has been run; otherwise flagged.
Anything the LLM invents (extra columns, bad keys, unparseable dtypes) is dropped
and logged in `DesignResult.notes`.

    python -m factory.agents.design runs/<run_id>      # reads discovery/, writes design/
"""
from __future__ import annotations

import csv
import re
import sys
from typing import Optional

from pydantic import BaseModel

from factory import catalog
from factory.config import ROOT, load_env
from factory.contracts.models import (ColumnContract, ColumnLineage, DataContract, DesignSpec,
                                      DiscoveryResult, Layer)
from factory.llm.base import LLM, get_llm

DDL = ROOT / "data" / "ddl" / "create_tables.sql"
GOLDEN = ROOT / "legacy" / "golden"
AGG = re.compile(r"\b(SUM|COUNT|AVG|MIN|MAX)\s*\(", re.I)
DTYPE = re.compile(r"^(string|boolean|int|bigint|double|date|timestamp|decimal\(\d{1,2},\s*\d{1,2}\))$")

Types = dict[str, tuple[str, bool]]          # "schema.table.column" -> (spark dtype, nullable)


class DesignResult(BaseModel):
    spec: DesignSpec
    notes: list[str] = []


# ============================================================== rules

def layer_and_name(table: str) -> tuple[Layer, str]:
    """rpt.x / legacy.rpt_x -> gold.x;  stg.x / legacy.stg_x / legacy.x -> silver.x."""
    return catalog.layer_and_name(table)


_SQL_TO_SPARK = {"varchar": "string", "char": "string", "date": "date", "datetime": "timestamp",
                 "int": "int", "bigint": "bigint", "float": "double", "bit": "boolean"}


def source_types() -> Types:
    """'dbo.table.column' -> (spark dtype, nullable), read from the legacy DDL."""
    out: Types = {}
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


def source_columns_in_order() -> dict[str, list[str]]:
    """'dbo.table' -> columns in DDL order (the first column is the table's identifier)."""
    out: dict[str, list[str]] = {}
    for full in source_types():
        s, t, c = full.split(".")
        out.setdefault(f"{s}.{t}", []).append(c)
    return out


def _scale(dtype: str) -> int:
    m = re.match(r"decimal\(\d+,\s*(\d+)\)", dtype)
    return int(m.group(1)) if m else 0


def infer_dtype(lin: ColumnLineage, types: Types) -> tuple[str, bool]:
    """Baseline (dtype, nullable) from the legacy expression and its source columns.

    Follows SQL Server result types: SUM(decimal(p,s)) -> decimal(38,s), ROUND(x, n) keeps
    a decimal with n places, DATEDIFF/YEAR -> int, COUNT -> bigint (Spark's count type).
    """
    tr = lin.transformation.strip()
    up = tr.upper()

    def resolve(src: str):
        # staging tables designed upstream are in `types`; when a job is designed on its
        # own, stg.x.col falls back to the raw dbo.x.col it mirrors
        return types.get(src) or types.get(re.sub(r"^\w+\.", "dbo.", src))

    known = [t for t in (resolve(s) for s in lin.source_columns) if t]
    decimals = [t for t, _ in known if t.startswith("decimal")]
    single = known[0] if len(lin.source_columns) == 1 and len(known) == 1 else None

    if re.match(r"^COUNT\s*\(", up):
        return "bigint", False
    m = re.search(r"\bAS\s+(?:DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*\)\s*$", up)
    if m:
        return f"decimal({m.group(1)},{m.group(2)})", True
    if re.match(r"^DATEFROMPARTS\s*\(", up):
        return "date", True
    if re.match(r"^(DATEDIFF|YEAR|MONTH|DAY)\s*\(", up):
        return "int", True
    if re.match(r"^CASE\b.*\bTHEN\s+'", up, re.S):
        return "string", True
    if up.startswith("COALESCE(") and single:
        return single[0], False
    if decimals:
        m = re.match(r"^ROUND\s*\(.*,\s*(\d+)\s*\)$", up, re.S)
        if m:
            return f"decimal(38,{m.group(1)})", True
        if re.search(r"[*/]", up):
            return "decimal(38,6)", True
        if AGG.search(up) or re.search(r"[+\-]|\bCASE\b", up):
            return f"decimal(38,{max(_scale(d) for d in decimals)})", True
    if single and not re.search(r"[+\-*/]|\bCASE\b|\(", tr):        # plain copy
        return single
    if single and re.match(r"^(UPPER|LOWER|LTRIM|RTRIM|TRIM)\s*\(", up) and single[0] == "string":
        return single
    if single and AGG.search(up):                                    # MIN / MAX keep the type
        return single[0], True
    name = lin.target_column
    computed = not lin.source_columns or not single or tr.lower() != lin.source_columns[0].split(".")[-1]
    if computed and re.search(r"(^|_)(year|days|count)(_|$)", name) and all(t == "date" for t, _ in known):
        return "int", True              # date arithmetic or a generated year / day count
    return "string", True


def baseline_columns(r: DiscoveryResult, table: str, types: Types) -> list[ColumnContract]:
    cols = []
    for lin in (c for c in r.column_lineage if c.target_table == table):
        dtype, nullable = infer_dtype(lin, types)
        cols.append(ColumnContract(name=lin.target_column, dtype=dtype, nullable=nullable))
    return cols


def fallback_key(r: DiscoveryResult, table: str, cols: list[ColumnContract],
                 table_columns: dict[str, list[str]]) -> list[str]:
    lins = [c for c in r.column_lineage if c.target_table == table]
    if any(AGG.search(c.transformation) for c in lins):          # aggregate -> GROUP BY columns
        return [c.target_column for c in lins if not AGG.search(c.transformation)]
    ids = []
    for c in lins:                                               # copies of an input table's id
        if len(c.source_columns) == 1:
            s, t, col = c.source_columns[0].split(".")
            order = table_columns.get(f"{s}.{t}", [])
            if order and order[0] == col == c.target_column:
                ids.append(c.target_column)
    generated = [c.target_column for c in lins if not c.source_columns
                 and not re.search(r"count|sum|total", c.transformation, re.I)]
    return ids + generated if ids else [c.name for c in cols]


def golden_key(table: str, cols: list[str]) -> list[str]:
    """Shortest run of leading columns that is unique in the legacy output (empty if none)."""
    path = GOLDEN / f"{table.split('.', 1)[1]}.csv"
    if not path.exists():
        return []
    with path.open(newline="") as f:
        rows = list(csv.reader(f))[1:]
    for n in range(1, min(len(cols), 4) + 1):
        if len({tuple(r[:n]) for r in rows}) == len(rows):
            return cols[:n]
    return []


def golden_header_columns(table: str, notes: list[str]) -> list[ColumnContract]:
    """SAS output with no lineage: read columns (and rough types) from the golden CSV."""
    name = table.split(".", 1)[1]
    path = GOLDEN / f"{name}.csv"
    if not path.exists():
        notes.append(f"no lineage and no golden output for {table}: run discovery with an LLM "
                     f"or run the SAS job first")
        return []
    with path.open(newline="") as f:
        rows = list(csv.reader(f))
    header, sample = rows[0], rows[1:200]
    cols = []
    for i, col in enumerate(header):
        vals = [r[i] for r in sample if i < len(r) and r[i].strip() not in ("", ".")]
        if vals and all(re.fullmatch(r"\d{4}-\d{2}-\d{2}", v) for v in vals):
            dtype = "date"
        elif vals and all(re.fullmatch(r"-?\d+", v) for v in vals):
            dtype = "bigint"
        elif vals and all(re.fullmatch(r"-?\d*\.\d+", v) or re.fullmatch(r"-?\d+", v) for v in vals):
            dtype = f"decimal(38,{max(len(v.split('.')[1]) if '.' in v else 0 for v in vals)})"
        else:
            dtype = "string"
        cols.append(ColumnContract(name=col.lower(), dtype=dtype, nullable=True))
    notes.append(f"{table}: columns taken from golden output header (no lineage available)")
    return cols


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


def design_table(r: DiscoveryResult, table: str, llm: LLM, types: Types,
                 table_columns: dict[str, list[str]], notes: list[str]) -> tuple[DataContract, str]:
    layer, new_name = layer_and_name(table)
    base = baseline_columns(r, table, types)
    from_golden = not base
    if from_golden:
        base = golden_header_columns(table, notes)
    by_name = {c.name: c for c in base}
    key = fallback_key(r, table, base, table_columns)
    if from_golden and base:
        key = golden_key(table, [c.name for c in base]) or key
        notes.append(f"{new_name}: primary key {key} found from uniqueness in the legacy output")

    if llm.available and r.column_lineage:
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
                         f"using fallback key {key}")
    elif not llm.available:
        notes.append("no LLM configured: contract built from parser baseline only")

    for k in key:                                   # a key column can never be null
        if k in by_name:
            by_name[k] = by_name[k].model_copy(update={"nullable": False})
    return DataContract(table=new_name, primary_key=[k for k in key if k in by_name],
                        columns=list(by_name.values())), new_name


# ============================================================== main

def design(r: DiscoveryResult, llm: Optional[LLM] = None, types: Optional[Types] = None,
           table_columns: Optional[dict[str, list[str]]] = None) -> DesignResult:
    """Design one job. Pass `types` / `table_columns` shared across jobs (in wave order) so
    staging tables designed upstream supply the types of their columns downstream."""
    llm = llm or get_llm()
    types = types if types is not None else source_types()
    table_columns = table_columns if table_columns is not None else source_columns_in_order()
    notes: list[str] = []
    contracts, names, layers = [], [], set()
    for table in r.output_tables:
        contract, name = design_table(r, table, llm, types, table_columns, notes)
        contracts.append(contract)
        names.append(name)
        layers.add(layer_and_name(table)[0])
        table_columns[table] = [c.name for c in contract.columns]   # feed downstream jobs
        for c in contract.columns:
            types[f"{table}.{c.name}"] = (c.dtype, c.nullable)
    layer = Layer.GOLD if Layer.GOLD in layers else Layer.SILVER
    return DesignResult(spec=DesignSpec(job_id=r.job_id, layer=layer, target_tables=names,
                                        contracts=contracts), notes=notes)


def main() -> None:
    load_env()
    run_dir = ROOT / sys.argv[1]
    llm = get_llm()
    out = run_dir / "design"
    out.mkdir(exist_ok=True)
    results = [DiscoveryResult.model_validate_json(f.read_text())
               for f in sorted((run_dir / "discovery").glob("J*.json"))]
    catalog.check_no_collisions([t for r in results for t in r.output_tables])
    types, table_columns = source_types(), source_columns_in_order()
    for r in sorted(results, key=lambda x: (x.wave or 0, x.job_id)):     # upstream first
        res = design(r, llm, types, table_columns)
        (out / f"{r.job_id}.json").write_text(res.model_dump_json(indent=2))
        print(f"  {r.job_id:34s} {res.spec.layer.value:7s} {', '.join(res.spec.target_tables)}"
              f"  key={res.spec.contracts[0].primary_key if res.spec.contracts else []}")
    from factory import mapping_excel
    mapping_excel.refresh(run_dir)


if __name__ == "__main__":
    sys.exit(main())
