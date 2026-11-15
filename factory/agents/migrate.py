"""
Migration agent: builds the silver and gold layers by having the LLM write each job's
PySpark code, running it, checking the result, and feeding failures back until it passes.

For every legacy job, as soon as its upstream jobs have passed:

    generate (LLM) -> safety check -> run on lakehouse inputs -> check output
        |                                                          |
        +------ failed: error / diff sent back to the LLM <--------+   (3 attempts, up to 6
        |       with the INPUT rows behind the differing rows          while the diff shrinks)
        passed: written to silver.<t> / gold.<t>; downstream jobs can now run

  * Two lanes run at the same time. LOCAL: the main model takes the next job whose upstream jobs
    have passed. HOSTED (--escalate): a job the main model cannot get to reconcile carries on
    there with everything it has tried, while the local lane moves on to the next job.
  * Lessons memory (factory.agents.lessons): every error is written down when it happens and
    every proven fix when an attempt passes; all later prompts, on both lanes, are told.
  * Inputs come from the lakehouse via factory.catalog (dbo.x -> bronze.x, stg.x -> silver.x),
    so the generated code keeps the legacy table names it was written against.
  * "Check output" = the data contract, plus the legacy output when legacy/golden/ has it.
  * A job whose upstream job failed in this run is BLOCKED (not run).
  * Generated code is checked before it runs: no file, network, process or eval access.

    python -m factory.bronze                         # once: raw CSVs -> bronze
    python -m factory.agents.migrate runs/<run_id>   # needs discovery/ and design/
    python -m factory.agents.migrate runs/<run_id> J05 J12   # selected jobs only
    python -m factory.agents.migrate runs/<run_id> --failed  # rerun only jobs that did not pass
                                                             # last time (passed ones are kept)
    ... --no-tests        # skip writing test_job.py for passed jobs (one LLM call per job saved)
    ... --no-dashboard    # do not build and open the review dashboard when the run ends
    ... --escalate=openrouter   # (or LLM_ESCALATE=openrouter) a job the main model cannot get to
                                # reconcile is handed to this second model, with everything tried so
                                # far. Local model first, hosted quota only for the hard jobs.
"""
from __future__ import annotations

import ast
import importlib.util
import json
import os
import re
import shutil
import sys
import time
import traceback
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from factory import catalog
from factory.agents import reconcile
from factory.agents.design import DesignResult
from factory.agents.generate import generate, generate_tests
from factory.agents.lessons import Lessons
from factory.config import ROOT, load_env
from factory.contracts.models import (DesignSpec, DiscoveryResult, JobRecord, JobStatus, LegacyJob,
                                      ReconciliationReport, TableDiff)
from factory.llm.base import LLM, LLMBudgetError, get_llm
from factory.runtime import lakehouse
from factory.runtime.spark import engine_name

MAX_ATTEMPTS = 3          # every job gets this many
MAX_EXTRA_ATTEMPTS = 3    # more, one at a time, only while each attempt gets closer to legacy
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


def _error_summary(e: BaseException, job_py: Path) -> str:
    """What the LLM needs to fix a crash: the exception message (Spark puts the cause first and the
    JVM stack trace after it, so the HEAD is kept, not the tail) and the lines of job.py involved."""
    msg = str(e).split("JVM stacktrace:")[0].strip()
    head = f"{type(e).__name__}: {msg[:1200]}"
    frames = [f for f in traceback.extract_tb(e.__traceback__) if Path(f.filename) == job_py]
    where = "\n".join(f"  job.py line {f.lineno}: {(f.line or '').strip()}" for f in frames[-3:])
    return head + (f"\nraised at:\n{where}" if where else "")


def distance(report: ReconciliationReport) -> Optional[int]:
    """How far an attempt is from the legacy output: rows that differ plus the row-count gap.
    None when it never got as far as a comparison (crash, rejected code, contract violation)."""
    if any(t.contract_violations for t in report.tables) or not report.golden_checked:
        return None
    return sum(t.mismatched_keys + abs(t.legacy_rows - t.migrated_rows) for t in report.tables)


def _load_run(path: Path, tag: str):
    spec = importlib.util.spec_from_file_location(f"migrated_{tag}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.run


# ============================================================== one job

@dataclass
class Trail:
    """Everything one job has been through. Passing the same Trail to a second migrate_job call
    lets another model carry on where the first stopped (escalation), instead of starting cold."""
    reports: list = field(default_factory=list)
    notes: list = field(default_factory=list)
    tried: list = field(default_factory=list)       # code of every failed attempt (a repeat is rejected)
    history: list = field(default_factory=list)     # "attempt N: why it failed", shown to the model
    prev_code: Optional[str] = None
    report: Optional[ReconciliationReport] = None
    best: Optional[int] = None                      # smallest distance from legacy so far
    attempt: int = 0
    seconds: float = 0.0


def migrate_job(spark, job: LegacyJob, disc: DiscoveryResult, spec: DesignSpec, llm: LLM,
                out_root: Path, log=print, tests: bool = True, trail: Optional[Trail] = None,
                test_llm: Optional[LLM] = None, lessons: Optional[Lessons] = None,
                ) -> tuple[JobRecord, list[ReconciliationReport], list[str]]:
    """trail: state from an earlier call for this job (attempt numbers continue from it).
    test_llm: model that writes the unit tests after a pass (default: llm).
    lessons: shared memory of mistakes; read before every attempt, updated after every attempt."""
    lang = job.language.value
    legacy_of = {catalog.lakehouse_name(t): t for t in disc.output_tables}
    first_table = spec.target_tables[0] if spec.target_tables else "?"
    tr = trail if trail is not None else Trail()
    reports, notes, tried, history = tr.reports, tr.notes, tr.tried, tr.history
    prev_code, report, best, attempt = tr.prev_code, tr.report, tr.best, tr.attempt
    limit = attempt + MAX_ATTEMPTS
    cap = limit + MAX_EXTRA_ATTEMPTS
    t_job = time.time()
    record = JobRecord(job_id=job.job_id, language=job.language, wave=disc.wave, layer=spec.layer,
                       attempts=attempt)

    while attempt < limit:
        attempt += 1
        t0 = time.time()
        record.attempts = attempt
        try:
            g = generate(job, disc, spec, llm, attempt=attempt, prev_code=prev_code, report=report,
                         out_root=out_root, tried=tried, history=history,
                         lessons=lessons.for_prompt(job.job_id, lang) if lessons else "")
            notes += [f"attempt {attempt}: {n}" for n in g.translation_notes]
        except LLMBudgetError as e:                     # hosted quota used up: stop cleanly
            attempt -= 1
            record.attempts = attempt
            record.status = JobStatus.NEEDS_REVIEW
            notes.append(f"stopped before attempt {attempt + 1}: {e}")
            log(f"    stopped: {e}")
            break
        except ValueError as e:                         # LLM never produced valid files
            fail = _failure(job.job_id, attempt, first_table, f"no usable code: {e}", t0)
            reports.append(fail)
            if prev_code is None:        # otherwise keep repairing from the last code that ran
                report = fail
            history.append(f"attempt {attempt}: no usable code ({str(e)[-160:]})")
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
                inputs = lakehouse.load_inputs(spark, disc.input_tables)
                outputs = run(spark, inputs)
                if not isinstance(outputs, dict):
                    raise TypeError(f"run() must return a dict of DataFrames, got {type(outputs).__name__}")
                to_lake = {legacy.lower(): lake for lake, legacy in legacy_of.items()}
                outputs = {to_lake.get(k.lower(), k.lower()): v for k, v in outputs.items()}   # stg.x -> silver.x
                report = reconcile.check(spark, job.job_id, attempt, outputs, spec.contracts, legacy_of, t0)
                if report.passed:
                    for c in spec.contracts:
                        df = outputs[c.table]
                        df = df.toDF(*[x.lower() for x in df.columns]).select(*[x.name for x in c.columns])
                        lakehouse.write_table(df, c.table)
                else:
                    try:                     # evidence for the repair prompt; never a reason to fail
                        reconcile.add_evidence(report, inputs, spec.contracts)
                    except Exception as e:
                        notes.append(f"attempt {attempt}: input rows not attached ({type(e).__name__})")
            except Exception as e:
                report = _failure(job.job_id, attempt, first_table,
                                  "runtime error:\n" + _error_summary(e, job_py), t0)
        reports.append(report)

        if report.passed:
            record.status = (JobStatus.VALIDATED if not report.golden_checked
                             else JobStatus.MATCHED if attempt == 1 else JobStatus.REPAIRED)
            log(f"    attempt {attempt}: passed ({'legacy output' if report.golden_checked else 'contract only'})"
                f"  {report.runtime_seconds}s")
            if lessons and history:              # what this attempt changed is now a proven fix
                fix = " ".join(n for n in g.translation_notes if n.startswith(("diagnosis:", "change:")))
                lessons.fixed(job.job_id, lang, history[-1].split(": ", 1)[-1], fix, f"{llm.provider}/{llm.model}")
            if tests:
                try:
                    err = generate_tests(job, test_llm or llm, out_root)
                except Exception as e:          # tests are a deliverable, not a gate: never fail the job
                    err = f"{type(e).__name__}: {e}"
                notes.append(f"unit tests: {'not written - ' + err if err else 'written to test_job.py'}")
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
        tried.append(code)
        history.append(f"attempt {attempt}: {why[:200]}")
        if lessons and first:                    # an error, not a row difference: remember it now
            lessons.failure(job.job_id, lang, why)
        d = distance(report)
        closer = d is not None and (best is None or d < best)
        if d is not None:
            best = d if best is None else min(best, d)
        if closer and attempt == limit and limit < cap:
            limit += 1                   # still converging on the legacy output: one more try
            log(f"    closer to legacy than any earlier attempt ({d} rows off): extra attempt {limit}")
    else:
        record.status = JobStatus.NEEDS_REVIEW
    tr.prev_code, tr.report, tr.best, tr.attempt = prev_code, report, best, attempt
    tr.seconds += time.time() - t_job
    record.agent_seconds = round(tr.seconds, 1)
    return record, reports, notes


# ============================================================== all jobs

def _second_session(spark):
    """Own Spark session for the hosted lane, so temp views made by two jobs running at the same
    time cannot collide. Falls back to the shared session where a second one is not available."""
    try:
        return spark.newSession()
    except Exception:
        return spark


def migrate(run_dir: Path, llm: Optional[LLM] = None, spark=None, only: Optional[set[str]] = None,
            out_root: Path = ROOT / "migrated", tests: bool = True,
            escalate: Optional[LLM] = None, lessons: Optional[Lessons] = None) -> list[JobRecord]:
    """escalate: second model for jobs that llm cannot get to reconcile (see module docstring).
    lessons: shared memory of mistakes (default: runs/lessons.json)."""
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

    lessons = lessons if lessons is not None else Lessons()
    order = sorted(disc, key=lambda k: (disc[k].wave or 0, k))
    todo = [j for j in order if not only or j[:3] in only]
    prev = {}                                    # last run's records: kept for jobs not rerun now
    if (out / "summary.json").exists():
        prev = {j["job_id"]: JobRecord.model_validate(j)
                for j in json.loads((out / "summary.json").read_text())["jobs"]}
    for f in out.glob("J*.json"):                # a stopped run has no summary: each settled job has its own file
        prev[f.stem] = JobRecord.model_validate(json.loads(f.read_text())["record"])
    status: dict[str, JobStatus] = {j: prev[j].status for j in order if j not in todo and j in prev}
    final: dict[str, JobRecord] = {j: prev[j] for j in status}
    work: dict[str, dict] = {}                   # per job: trail, reports, notes, model used

    def say(jid: str, lane: str):
        return lambda msg: print(f"  {jid[:3]} {lane:6s}| {str(msg).strip()}", flush=True)

    def save(jid: str, rec: JobRecord) -> None:
        w = work.get(jid, {})
        used = w.get("used")
        (out / f"{jid}.json").write_text(json.dumps({
            "record": rec.model_dump(mode="json"),
            "llm": f"{used.provider}/{used.model}" if used and w.get("reports") else None,
            "escalated_from": w.get("handed_over"),
            "reports": [r.model_dump(mode="json") for r in w.get("reports", [])],
            "notes": w.get("notes", [])}, indent=2, default=str))

    def settle(jid: str, rec: JobRecord) -> None:
        status[jid], final[jid] = rec.status, rec
        save(jid, rec)

    def run_lane(jid: str, lane: str, model: LLM, session) -> JobRecord:
        """One job on one lane. Never raises: an agent error leaves the job for review."""
        w = work[jid]
        try:
            rec, w["reports"], w["notes"] = migrate_job(
                session, jobs[jid], disc[jid], des[jid], model, out_root, log=say(jid, lane),
                tests=tests and lane == "local", trail=w["trail"], lessons=lessons)
            w["used"] = model
        except Exception as e:                   # lost connection, hosted model down, ...
            w["trail"].notes.append(f"{lane} lane stopped: {type(e).__name__}: {str(e)[:200]}")
            say(jid, lane)(f"stopped: {type(e).__name__}: {str(e)[:150]}")
            rec = JobRecord(job_id=jid, language=jobs[jid].language, wave=disc[jid].wave, layer=des[jid].layer,
                            status=JobStatus.NEEDS_REVIEW, attempts=w["trail"].attempt,
                            agent_seconds=round(w["trail"].seconds, 1))
            w["reports"], w["notes"] = w["trail"].reports, w["trail"].notes
        return rec

    # Two lanes at once. LOCAL: the main model takes the next job whose upstream jobs have passed.
    # HOSTED: a job the main model could not get to reconcile carries on there, with everything it
    # has tried, while the local lane moves on. A job is BLOCKED only once an upstream job has
    # failed on every lane it could use.
    hosted_spark = _second_session(spark) if escalate is not None else spark
    local_pool, hosted_pool = ThreadPoolExecutor(1), ThreadPoolExecutor(1)
    pending, running, local_busy = list(todo), {}, False
    while pending or running:
        moved = False
        for jid in list(pending):
            d = disc[jid]
            failed_deps = [x for x in d.depends_on if x in status and status[x] not in OK]
            waiting = [x for x in d.depends_on if x not in status and x in todo]
            if failed_deps:
                pending.remove(jid)
                work[jid] = {"notes": [f"blocked by {failed_deps}"]}
                settle(jid, JobRecord(job_id=jid, language=jobs[jid].language, wave=d.wave,
                                      layer=des[jid].layer, status=JobStatus.BLOCKED))
                say(jid, "")(f"blocked: upstream {', '.join(x[:3] for x in failed_deps)} did not pass")
                moved = True
            elif not waiting and not local_busy:
                pending.remove(jid)
                work[jid] = {"trail": Trail()}
                say(jid, "local")(f"start  wave {d.wave}  -> {', '.join(des[jid].target_tables)}")
                running[local_pool.submit(run_lane, jid, "local", llm, spark)] = (jid, "local")
                local_busy = moved = True
        if moved:
            continue                             # a block or a start may free other jobs
        if not running:                          # nothing can start: dependencies that never settle
            for jid in pending:
                work[jid] = {"notes": ["not run: its upstream jobs never finished"]}
                settle(jid, JobRecord(job_id=jid, language=jobs[jid].language, wave=disc[jid].wave,
                                      layer=des[jid].layer, status=JobStatus.BLOCKED))
            break
        done, _ = wait(running, return_when=FIRST_COMPLETED)
        for f in done:
            jid, lane = running.pop(f)
            rec = f.result()
            if lane == "local":
                local_busy = False
                left = getattr(escalate, "budget_left", None) if escalate is not None else None
                if rec.status == JobStatus.NEEDS_REVIEW and escalate is not None and (left is None or left > 0):
                    w = work[jid]
                    w["handed_over"] = f"{llm.provider}/{llm.model}"
                    w["trail"].notes.append(f"escalated to {escalate.provider}/{escalate.model} after "
                                            f"{rec.attempts} attempts on {w['handed_over']}")
                    say(jid, "hosted")(f"escalated to {escalate.provider}/{escalate.model}"
                                       + (f"  ({left} requests left today)" if left is not None else ""))
                    running[hosted_pool.submit(run_lane, jid, "hosted", escalate, hosted_spark)] = (jid, "hosted")
                    continue
                if rec.status == JobStatus.NEEDS_REVIEW and escalate is not None:
                    work[jid]["notes"].append(f"not escalated: {escalate.provider} daily budget is used up")
                    say(jid, "local")(f"not escalated: {escalate.provider} daily budget is used up")
            settle(jid, rec)
    local_pool.shutdown()
    hosted_pool.shutdown()

    if tests:                                    # jobs fixed on the hosted lane: the local model writes
        for jid, w in work.items():              # their tests, so no hosted quota is spent on them
            if w.get("handed_over") and status.get(jid) in OK and w.get("used") is escalate:
                try:
                    err = generate_tests(jobs[jid], llm, out_root)
                except Exception as e:
                    err = f"{type(e).__name__}: {e}"
                w["notes"].append(f"unit tests: {'not written - ' + err if err else 'written to test_job.py'}")
                save(jid, final[jid])

    records = [final[j] for j in order if j in final]
    summary = {s.value: sum(r.status == s for r in records) for s in JobStatus if any(r.status == s for r in records)}
    (out / "summary.json").write_text(json.dumps({"jobs": [r.model_dump(mode="json") for r in records],
                                                  "by_status": summary}, indent=2))
    print("\n  " + "  ".join(f"{k}: {v}" for k, v in summary.items()))
    return records


OK = {JobStatus.VALIDATED, JobStatus.MATCHED, JobStatus.REPAIRED}


def failed_jobs(run_dir: Path) -> set[str]:
    """Job prefixes (J04, ...) that did not pass in the last run of run_dir."""
    jobs = json.loads((run_dir / "migrate" / "summary.json").read_text())["jobs"]
    return {j["job_id"][:3] for j in jobs if JobStatus(j["status"]) not in OK}


def main() -> None:
    load_env()
    run_dir = ROOT / sys.argv[1]
    llm = get_llm()
    print(f"Migration run {run_dir.name}  (LLM {llm.provider}/{llm.model}, engine {engine_name()})")
    args = sys.argv[2:]
    flags = {a for a in args if a.startswith("--")}
    only = failed_jobs(run_dir) if "--failed" in flags else {a for a in args if a not in flags}
    if "--failed" in flags:
        print(f"  rerunning jobs that did not pass: {', '.join(sorted(only)) or 'none'}")
    name = next((f.split("=", 1)[1] for f in flags if f.startswith("--escalate=")), None) \
        or os.getenv("LLM_ESCALATE")
    escalate = None
    if name and name != llm.provider:
        escalate = get_llm(name)
        if not escalate.available:
            print(f"  escalation to {name} is off: its API key is not set")
            escalate = None
        else:
            left = getattr(escalate, "budget_left", None)
            print(f"  jobs that fail on {llm.provider} escalate to {escalate.provider}/{escalate.model}"
                  + (f"  ({left} requests left today)" if left is not None else ""))
    migrate(run_dir, llm, only=only or None, tests="--no-tests" not in flags, escalate=escalate)
    if "--no-dashboard" not in flags:            # the review page for this run, opened in the browser
        try:
            from factory.agents import dashboard
            page = dashboard.build(run_dir)
            opened = dashboard.show(page)
            print(f"  dashboard: {page.relative_to(ROOT)}" + ("  (opened in the browser)" if opened else ""))
        except Exception as e:                   # the run itself is done: never fail it here
            print(f"  dashboard not built: {type(e).__name__}: {e}")


if __name__ == "__main__":
    sys.exit(main())
