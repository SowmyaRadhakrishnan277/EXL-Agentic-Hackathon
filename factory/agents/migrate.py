"""
Migration agent: builds the silver and gold layers by having the LLM write each job's
PySpark code, running it, checking the result, and feeding failures back until it passes.

For every legacy job, in wave order:

    generate (LLM) -> safety check -> run on lakehouse inputs -> check output
        |                                                          |
        +------ failed: error / diff sent back to the LLM <--------+   (max 3 attempts)
        passed: written to silver.<t> / gold.<t>; downstream jobs can now run

  * Inputs come from the lakehouse via factory.catalog (dbo.x -> bronze.x, stg.x -> silver.x),
    so the generated code keeps the legacy table names it was written against.
  * "Check output" = the data contract, plus the legacy output when legacy/golden/ has it.
  * A job whose upstream job failed in this run is BLOCKED (not run).
  * Generated code is checked before it runs: no file, network, process or eval access.

    python -m factory.bronze                         # once: raw CSVs -> bronze
    python -m factory.agents.migrate runs/<run_id>   # needs discovery/ and design/
    python -m factory.agents.migrate runs/<run_id> J05 J12   # selected jobs only
"""
from __future__ import annotations

import ast
import importlib.util
import json
import re
import shutil
import sys
import time
import traceback
from pathlib import Path
from typing import Optional

from factory import catalog
from factory.agents import reconcile
from factory.agents.design import DesignResult
from factory.agents.generate import generate
from factory.config import ROOT, load_env
from factory.contracts.models import (DesignSpec, DiscoveryResult, JobRecord, JobStatus, LegacyJob,
                                      ReconciliationReport, TableDiff)
from factory.llm.base import LLM, get_llm
from factory.runtime import lakehouse
from factory.runtime.spark import engine_name

MAX_ATTEMPTS = 3
FORBIDDEN_MODULES = {"os", "sys", "subprocess", "socket", "shutil", "pathlib", "requests", "urllib",
                     "http", "ftplib", "smtplib", "pickle", "ctypes", "importlib", "builtins"}
FORBIDDEN_CALLS = {"open", "exec", "eval", "compile", "__import__", "input", "globals", "setattr"}


# ============================================================== guardrails

def safety_check(code: str) -> Optional[str]:
    """Reject generated code that reaches outside Spark (files, network, processes, eval)."""
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        return f"syntax error: {e.msg} (line {e.lineno})"
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            bad = [a.name for a in node.names if a.name.split(".")[0] in FORBIDDEN_MODULES]
            if bad:
                return f"forbidden import {bad}: generated jobs may only use pyspark, datetime, decimal, math, re"
        if isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[0] in FORBIDDEN_MODULES:
            return f"forbidden import from '{node.module}'"
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in FORBIDDEN_CALLS:
            return f"forbidden call {node.func.id}()"
    return None


def _failure(job_id: str, attempt: int, table: str, message: str, started: float) -> ReconciliationReport:
    return ReconciliationReport(job_id=job_id, attempt=attempt, passed=False, engine=engine_name(),
                                runtime_seconds=round(time.time() - started, 2),
                                tables=[TableDiff(table=table, legacy_rows=0, migrated_rows=0,
                                                  contract_violations=[message])])


def _load_run(path: Path, tag: str):
    spec = importlib.util.spec_from_file_location(f"migrated_{tag}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.run


# ============================================================== one job

def migrate_job(spark, job: LegacyJob, disc: DiscoveryResult, spec: DesignSpec, llm: LLM,
                out_root: Path, log=print) -> tuple[JobRecord, list[ReconciliationReport], list[str]]:
    legacy_of = {catalog.lakehouse_name(t): t for t in disc.output_tables}
    first_table = spec.target_tables[0] if spec.target_tables else "?"
    reports: list[ReconciliationReport] = []
    notes: list[str] = []
    prev_code: Optional[str] = None
    report: Optional[ReconciliationReport] = None
    t_job = time.time()
    record = JobRecord(job_id=job.job_id, language=job.language, wave=disc.wave, layer=spec.layer)

    for attempt in range(1, MAX_ATTEMPTS + 1):
        t0 = time.time()
        record.attempts = attempt
        try:
            g = generate(job, disc, spec, llm, attempt=attempt, prev_code=prev_code, report=report,
                         out_root=out_root)
            notes += [f"attempt {attempt}: {n}" for n in g.translation_notes]
        except ValueError as e:                         # LLM never produced valid files
            report = _failure(job.job_id, attempt, first_table, f"no usable code: {e}", t0)
            reports.append(report)
            log(f"    attempt {attempt}: no usable code")
            continue

        job_py = out_root / job.job_id / "job.py"
        code = job_py.read_text()
        hist = out_root / job.job_id / "attempts"
        hist.mkdir(exist_ok=True)
        shutil.copy(job_py, hist / f"job_attempt{attempt}.py")

        problem = safety_check(code)
        if problem:
            report = _failure(job.job_id, attempt, first_table, f"rejected before running: {problem}", t0)
        else:
            try:
                run = _load_run(job_py, f"{job.job_id}_{attempt}")
                outputs = run(spark, lakehouse.load_inputs(spark, disc.input_tables))
                if not isinstance(outputs, dict):
                    raise TypeError(f"run() must return a dict of DataFrames, got {type(outputs).__name__}")
                outputs = {k.lower(): v for k, v in outputs.items()}
                report = reconcile.check(spark, job.job_id, attempt, outputs, spec.contracts, legacy_of, t0)
                if report.passed:
                    for c in spec.contracts:
                        df = outputs[c.table]
                        df = df.toDF(*[x.lower() for x in df.columns]).select(*[x.name for x in c.columns])
                        lakehouse.write_table(df, c.table)
            except Exception:
                tb = traceback.format_exc()
                report = _failure(job.job_id, attempt, first_table, "runtime error:\n" + tb[-1500:], t0)
        reports.append(report)

        if report.passed:
            record.status = (JobStatus.VALIDATED if not report.golden_checked
                             else JobStatus.MATCHED if attempt == 1 else JobStatus.REPAIRED)
            log(f"    attempt {attempt}: passed ({'legacy output' if report.golden_checked else 'contract only'})"
                f"  {report.runtime_seconds}s")
            break
        first = next((v for t in report.tables for v in t.contract_violations), None)
        if first:
            lines = [x for x in first.splitlines() if x.strip()]
            errs = [x for x in lines if re.search(r"(Error|Exception)\b", x)]
            why = (errs[-1] if errs else lines[-1]).strip()
        else:
            why = f"{sum(t.mismatched_keys for t in report.tables)} rows differ from legacy output"
        log(f"    attempt {attempt}: failed - {why[:110]}")
        prev_code = code
    else:
        record.status = JobStatus.NEEDS_REVIEW
    record.agent_seconds = round(time.time() - t_job, 1)
    return record, reports, notes


# ============================================================== all jobs

def migrate(run_dir: Path, llm: Optional[LLM] = None, spark=None, only: Optional[set[str]] = None,
            out_root: Path = ROOT / "migrated") -> list[JobRecord]:
    from factory.agents.discovery import load_jobs
    from factory.runtime.spark import get_spark

    llm = llm or get_llm()
    if not llm.available:
        raise RuntimeError("the migration agent needs an LLM: set LLM_PROVIDER=gemini and GEMINI_API_KEY")
    spark = spark or get_spark()
    lakehouse.ensure_schemas(spark)
    jobs = {j.job_id: j for j in load_jobs()}
    disc = {f.stem: DiscoveryResult.model_validate_json(f.read_text())
            for f in (run_dir / "discovery").glob("J*.json")}
    des = {f.stem: DesignResult.model_validate_json(f.read_text()).spec
           for f in (run_dir / "design").glob("J*.json")}
    out = run_dir / "migrate"
    out.mkdir(exist_ok=True)
    out_root.mkdir(parents=True, exist_ok=True)
    (out_root / "__init__.py").touch()

    ok = {JobStatus.VALIDATED, JobStatus.MATCHED, JobStatus.REPAIRED}
    status: dict[str, JobStatus] = {}
    records = []
    for jid in sorted(disc, key=lambda k: (disc[k].wave or 0, k)):
        if only and jid[:3] not in only:
            continue
        d = disc[jid]
        print(f"  wave {d.wave}  {jid}  -> {', '.join(des[jid].target_tables)}")
        failed_deps = [x for x in d.depends_on if x in status and status[x] not in ok]
        if failed_deps:
            rec = JobRecord(job_id=jid, language=jobs[jid].language, wave=d.wave, layer=des[jid].layer,
                            status=JobStatus.BLOCKED)
            reports, notes = [], [f"blocked by {failed_deps}"]
            print(f"    blocked: upstream {', '.join(x[:3] for x in failed_deps)} did not pass")
        else:
            rec, reports, notes = migrate_job(spark, jobs[jid], d, des[jid], llm, out_root)
        status[jid] = rec.status
        records.append(rec)
        (out / f"{jid}.json").write_text(json.dumps({
            "record": rec.model_dump(mode="json"),
            "reports": [r.model_dump(mode="json") for r in reports],
            "notes": notes}, indent=2, default=str))

    summary = {s.value: sum(r.status == s for r in records) for s in JobStatus if any(r.status == s for r in records)}
    (out / "summary.json").write_text(json.dumps({"jobs": [r.model_dump(mode="json") for r in records],
                                                  "by_status": summary}, indent=2))
    print("\n  " + "  ".join(f"{k}: {v}" for k, v in summary.items()))
    return records


def main() -> None:
    load_env()
    run_dir = ROOT / sys.argv[1]
    llm = get_llm()
    print(f"Migration run {run_dir.name}  (LLM {llm.provider}/{llm.model}, engine {engine_name()})")
    migrate(run_dir, llm, only=set(sys.argv[2:]) or None)
    from factory import mapping_excel
    mapping_excel.refresh(run_dir)


if __name__ == "__main__":
    sys.exit(main())
