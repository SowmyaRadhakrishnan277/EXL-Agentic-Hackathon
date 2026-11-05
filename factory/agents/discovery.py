"""
Discovery agent: legacy code in, lineage + dependencies + migration waves out.

Strategy ("parser first, LLM fills gaps"):
  T-SQL  -> sqlglot parses tables and column lineage exactly.
            The LLM only writes the plain-English summary and migration risk notes.
  SAS    -> a rule-based scanner finds input/output tables (infile, set, from, data ...).
            The LLM maps column lineage, constrained to the columns that really exist
            in the input tables; any column it invents is dropped and logged.
  Waves  -> job B depends on job A when B reads a table A writes; wave = depth in that graph.

    python -m factory.agents.discovery              # writes runs/<run_id>/discovery/
"""
from __future__ import annotations

import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage as sg_lineage

from factory.config import ROOT, load_env
from factory.contracts.models import (ColumnLineage, DiscoveryResult, LegacyJob,
                                      SourceLanguage)
from factory.llm.base import LLM, get_llm

DDL = ROOT / "data" / "ddl" / "create_tables.sql"
SAS_WORK = "work"


# ============================================================== loading

def load_jobs() -> list[LegacyJob]:
    jobs = []
    for lang, folder, ext in [(SourceLanguage.TSQL, "tsql", "sql"), (SourceLanguage.SAS, "sas", "sas")]:
        for p in sorted((ROOT / "legacy" / folder).glob(f"J*.{ext}")):
            jobs.append(LegacyJob(job_id=p.stem, language=lang,
                                  path=str(p.relative_to(ROOT)), code=p.read_text()))
    return sorted(jobs, key=lambda j: j.job_id)


def ddl_schema() -> dict[str, list[str]]:
    """Source tables and their columns, read from the legacy DDL."""
    out: dict[str, list[str]] = {}
    for batch in re.split(r"^\s*GO\s*$", DDL.read_text(), flags=re.M):   # GO separates batches
        for stmt in sqlglot.parse(batch, read="tsql"):
            if isinstance(stmt, exp.Create) and stmt.kind == "TABLE":
                t = stmt.this.this
                out[f"{t.db or 'dbo'}.{t.name}".lower()] = [c.name.lower() for c in stmt.this.expressions
                                                            if isinstance(c, exp.ColumnDef)]
    if not out:
        raise RuntimeError(f"no tables parsed from {DDL}")
    return out


def csv_to_table(stem: str) -> str:
    """Extract file naming rule: stg_x.csv -> stg.x, rpt_x.csv -> rpt.x, x.csv -> dbo.x."""
    stem = stem.lower()
    for schema in ("stg", "rpt"):
        if stem.startswith(schema + "_"):
            return f"{schema}.{stem[len(schema) + 1:]}"
    return f"dbo.{stem}"


# ============================================================== T-SQL

def _tsql_statement(code: str) -> exp.Select:
    code = re.sub(r"DECLARE\s+@(\w+)\s+\w+\s*=\s*('[^']*')\s*;",
                  lambda m: f"/*@{m.group(1)}={m.group(2)}*/", code, flags=re.I)
    vars_ = dict(re.findall(r"/\*@(\w+)=('[^']*')\*/", code))
    for k, v in vars_.items():
        code = code.replace(f"@{k}", f"CAST({v} AS DATE)")
    for stmt in sqlglot.parse(code, read="tsql"):
        if isinstance(stmt, exp.Select) and stmt.args.get("into"):
            return stmt
    raise ValueError("no SELECT ... INTO found")


def tsql_io(job: LegacyJob) -> tuple[list[str], list[str]]:
    sel = _tsql_statement(job.code)
    target = sel.args["into"].this
    out = f"{target.db}.{target.name}".lower()
    ctes = {c.alias_or_name.lower() for c in sel.find_all(exp.CTE)}
    ins = sorted({f"{t.db or 'dbo'}.{t.name}".lower() for t in sel.find_all(exp.Table)
                  if t.name and t.name.lower() not in ctes and f"{t.db}.{t.name}".lower() != out
                  and t is not target})
    return ins, [out]


def tsql_lineage(job: LegacyJob, schema: dict[str, list[str]]) -> tuple[list[ColumnLineage], list[str]]:
    sel = _tsql_statement(job.code)
    target = sel.args["into"].this
    out = f"{target.db}.{target.name}".lower()
    query = sel.copy()
    query.set("into", None)
    sg_schema: dict = {}
    for full, cols in schema.items():
        if not cols:
            continue
        db, t = full.split(".")
        sg_schema.setdefault(db, {})[t] = {c: "varchar" for c in cols}
    result = []
    for proj in query.expressions:
        col = proj.alias_or_name
        node = sg_lineage(col, query, schema=sg_schema, dialect="tsql")
        srcs = set()
        for n in node.walk():
            if not n.downstream and isinstance(n.source, exp.Table):
                t = n.source
                srcs.add(f"{t.db or 'dbo'}.{t.name}.{n.name.split('.')[-1]}".lower())
        expr = proj.this if isinstance(proj, exp.Alias) else proj
        result.append(ColumnLineage(target_table=out, target_column=col.lower(),
                                    source_columns=sorted(srcs),
                                    transformation=expr.sql(dialect="tsql")))
    return result, [p.alias_or_name.lower() for p in query.expressions]


# ============================================================== SAS

def _strip_sas_comments(code: str) -> str:
    code = re.sub(r"/\*.*?\*/", " ", code, flags=re.S)
    return re.sub(r"^\s*\*[^;]*;", " ", code, flags=re.M)


def sas_io(job: LegacyJob) -> tuple[list[str], list[str]]:
    code = _strip_sas_comments(job.code)
    ins, outs = set(), set()
    for f in re.findall(r'infile\s+"[^"]*/([\w]+)\.csv"', code, flags=re.I):
        ins.add(csv_to_table(f))
    for lib, t in re.findall(r"\b(?:set|from|join|data\s*=)\s*(\w+)\.(\w+)", code, flags=re.I):
        if lib.lower() != SAS_WORK:
            ins.add(f"{lib}.{t}".lower())
    for lib, t in re.findall(r"^\s*data\s+(\w+)\.(\w+)\s*;", code, flags=re.I | re.M):
        if lib.lower() != SAS_WORK:
            outs.add(f"{lib}.{t}".lower())
    for lib, t in re.findall(r"create\s+table\s+(\w+)\.(\w+)", code, flags=re.I):
        if lib.lower() != SAS_WORK:
            outs.add(f"{lib}.{t}".lower())
    return sorted(ins - outs), sorted(outs)


SAS_SYSTEM = """You are a data migration analyst. You read legacy SAS programs and
document column-level lineage precisely. Only use table and column names that appear
in the provided input schemas. Trace through intermediate WORK datasets back to the
real input tables (one hop: the input tables of this program). Reply with JSON only."""

SAS_PROMPT = """SAS program {job_id}:
```sas
{code}
```

Input tables of this program and their columns:
{schemas}

Output table(s): {outputs}

Return JSON:
{{
  "columns": [
    {{"target_table": "<output table>", "target_column": "<name>",
      "source_columns": ["<input_table>.<column>", ...],
      "transformation": "<short rule, e.g. 'sum of amount where payment_type = INDEMNITY'>"}}
  ],
  "output_columns": {{"<output table>": ["<every column, in order>"]}}
}}
Include every column of every output table. Use [] for source_columns of pure counts or constants."""


def sas_lineage(job: LegacyJob, inputs: list[str], outputs: list[str],
                schema: dict[str, list[str]], llm: LLM, log: list[str]):
    if not llm.available:
        log.append("no LLM configured: SAS column lineage skipped")
        return [], {}
    schemas = "\n".join(f"- {t}: {', '.join(schema.get(t, ['(unknown)']))}" for t in inputs)
    reply = llm.complete_json(SAS_SYSTEM, SAS_PROMPT.format(
        job_id=job.job_id, code=job.code, schemas=schemas, outputs=", ".join(outputs)))
    known = {f"{t}.{c}" for t in inputs for c in schema.get(t, [])}
    result = []
    for c in reply.get("columns", []):
        srcs = [s.lower() for s in c.get("source_columns", [])]
        bad = [s for s in srcs if s not in known]
        if bad:
            log.append(f"guardrail: dropped invented source columns {bad} for {c.get('target_column')}")
        result.append(ColumnLineage(target_table=c["target_table"].lower(),
                                    target_column=c["target_column"].lower(),
                                    source_columns=sorted(s for s in srcs if s in known),
                                    transformation=c.get("transformation", "")))
    out_cols = {k.lower(): [x.lower() for x in v] for k, v in reply.get("output_columns", {}).items()}
    return result, out_cols


# ============================================================== summaries

SUMMARY_SYSTEM = """You are a senior data engineer planning a migration from SQL Server
T-SQL and SAS to PySpark on a lakehouse. Be concise and specific. Reply with JSON only."""

SUMMARY_PROMPT = """Legacy {lang} job {job_id}:
```
{code}
```
Return JSON:
{{"summary": "<one or two sentences: what the job produces and for whom>",
  "risk_notes": ["<each behaviour that a straight PySpark translation could get wrong:
                   NULL handling, string comparison/collation, rounding, date functions,
                   de-duplication, type precision. Name the exact construct.>"]}}"""


def summarise(job: LegacyJob, llm: LLM) -> tuple[str, list[str]]:
    if not llm.available:
        return "", []
    r = llm.complete_json(SUMMARY_SYSTEM, SUMMARY_PROMPT.format(
        lang=job.language.value.upper(), job_id=job.job_id, code=job.code))
    return r.get("summary", ""), list(r.get("risk_notes", []))


# ============================================================== waves

def plan_waves(results: dict[str, DiscoveryResult]) -> None:
    writer = {t: j for j, r in results.items() for t in r.output_tables}
    for r in results.values():
        r.depends_on = sorted({writer[t] for t in r.input_tables if t in writer and writer[t] != r.job_id})
    wave: dict[str, int] = {}

    def depth(j: str, seen: tuple = ()) -> int:
        if j in seen:
            raise ValueError(f"dependency cycle: {seen + (j,)}")
        if j not in wave:
            wave[j] = 1 + max((depth(d, seen + (j,)) for d in results[j].depends_on), default=0)
        return wave[j]

    for j, r in results.items():
        r.wave = depth(j)


# ============================================================== main

def discover(llm: LLM | None = None, run_dir: Path | None = None) -> dict[str, DiscoveryResult]:
    llm = llm or get_llm()
    jobs = load_jobs()
    schema = ddl_schema()
    io = {j.job_id: (tsql_io(j) if j.language == SourceLanguage.TSQL else sas_io(j)) for j in jobs}
    results = {j.job_id: DiscoveryResult(job_id=j.job_id, input_tables=io[j.job_id][0],
                                         output_tables=io[j.job_id][1], column_lineage=[],
                                         summary="") for j in jobs}
    plan_waves(results)

    by_id = {j.job_id: j for j in jobs}
    for jid in sorted(results, key=lambda k: (results[k].wave, k)):   # upstream first
        job, r, log = by_id[jid], results[jid], []
        t0 = time.time()
        if job.language == SourceLanguage.TSQL:
            r.column_lineage, cols = tsql_lineage(job, schema)
            schema[r.output_tables[0]] = cols
            r.method = "sqlglot"
        else:
            r.column_lineage, out_cols = sas_lineage(job, r.input_tables, r.output_tables, schema, llm, log)
            for t in r.output_tables:
                schema[t] = out_cols.get(t) or sorted({c.target_column for c in r.column_lineage
                                                       if c.target_table == t})
            r.method = f"rules+{llm.provider}"
        r.summary, r.risk_notes = summarise(job, llm)
        r.risk_notes += log
        print(f"  {jid:34s} wave {r.wave}  {len(r.column_lineage):3d} cols  {time.time() - t0:5.1f}s")

    if run_dir:
        d = run_dir / "discovery"
        d.mkdir(parents=True, exist_ok=True)
        for jid, r in results.items():
            (d / f"{jid}.json").write_text(r.model_dump_json(indent=2))
        waves = {}
        for r in results.values():
            waves.setdefault(r.wave, []).append(r.job_id)
        (d / "waves.json").write_text(json.dumps({str(k): sorted(v) for k, v in sorted(waves.items())}, indent=2))
    return results


def main() -> None:
    load_env()
    llm = get_llm()
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = ROOT / "runs" / run_id
    print(f"Discovery run {run_id}  (LLM: {llm.provider}/{llm.model})")
    results = discover(llm, run_dir)
    print(f"\nWaves:")
    for w in sorted({r.wave for r in results.values()}):
        print(f"  wave {w}: {', '.join(sorted(j[:3] for j, r in results.items() if r.wave == w))}")
    print(f"\nWritten to {run_dir.relative_to(ROOT)}/discovery/")
    print(f"Score it:  python -m eval.score_discovery {run_dir.relative_to(ROOT)}")


if __name__ == "__main__":
    sys.exit(main())
