"""
Translation agent: legacy code + lineage + design in, PySpark job + unit tests out.

Every generated job has the same shape, so the reconciler and the tests can call it:

    def run(spark, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
        # tables: legacy input tables by name, e.g. "dbo.claims", "stg.policies"
        # returns: target tables by their new name, e.g. "gold.open_reserves"

Safety nets around the LLM:
  * the reply must be JSON with `job_py` (the unit tests come from generate_tests, once the job passes)
  * job_py must parse (ast) and job_py must define run(spark, tables)
  * if not, the error is fed back and the model retries (up to MAX_FIX times)
  * repair mode: pass the previous code and a ReconciliationReport; the diff is
    turned into a targeted prompt (used by the self-fix loop)

    python -m factory.agents.generate runs/<run_id>      # needs discovery/ and design/
"""
from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path
from typing import Optional

from factory.config import ROOT, load_env
from factory.contracts.models import (DesignSpec, DiscoveryResult, GeneratedJob, LegacyJob,
                                      ReconciliationReport)
from factory.llm.base import LLM, get_llm

MAX_FIX = 2          # syntax / shape retries inside one attempt

# Legacy behaviours where a straight translation silently changes results. Each trap is sent only
# when the legacy code uses the construct (pattern on the lower-cased code, None = always), which
# keeps the prompt short for a slow local model.
TRAPS: list[tuple[Optional[str], str]] = [
    (r"'[^']*'", """SQL Server compares strings case-insensitively and ignores TRAILING spaces; leading spaces still matter.
   `status = 'OPEN'` / `<> 'REJECTED'` therefore match 'open' and 'OPEN  ' but not ' Open'.
   In Spark use upper(rtrim(col)) on both sides. Keep untrimmed values in output columns unless the legacy code trims them."""),
    (None, """NULL: in T-SQL a comparison with NULL is unknown, so the row fails WHERE and falls to ELSE in CASE.
   SAS treats a missing number as lower than every number (`if amount < 100` is TRUE for missing).
   SAS `a + b` is missing if either is missing; SAS sum() ignores missing. Do not add coalesce(...,0) unless the legacy code does."""),
    (r"\bcount\s*\(|\bn\s*\(|\bnmiss\b", "COUNT(*) counts rows with NULLs; COUNT(col) skips NULLs; COUNT(DISTINCT col) skips NULLs."),
    (r"\bdistinct\b|nodupkey|nodup\b", "DISTINCT / SAS `proc sort nodupkey` remove duplicates BEFORE aggregating. Keep that order. nodupkey keeps one row per key."),
    (r"datediff|intck|datepart|\byear\s*\(", "DATEDIFF(YEAR, a, b) counts calendar-year boundaries (year(b) - year(a)), NOT elapsed years. DATEDIFF(DAY,a,b) = datediff(b, a)."),
    (r"\bround\s*\(|\bdecimal\b|\bnumeric\b|\d\.0\b|/", """ROUND(x, 2) in T-SQL and SAS rounds half AWAY from zero. Use Spark round() on DECIMAL columns, never bround(), Python round() or float.
   Decimal arithmetic: keep decimals; `x * n / 365.0` in T-SQL is decimal, so cast to decimal rather than double."""),
    (r"coalesce|isnull|\bnvl\b", "COALESCE/ISNULL defaults (e.g. end_date -> 9999-12-31) must be applied exactly where the legacy code applies them."),
    (r"group\s+by|full\s+(outer\s+)?join|\bclass\b|\bby\b", "GROUP BY on a NULL key forms its own group in both T-SQL and SAS; keep it (do not drop NULL-key rows). FULL JOIN keeps unmatched rows from both sides."),
    (None, "Output column names, order and types must match the data contract exactly."),
]


def traps_for(code: str) -> str:
    """The trap list for one job: only the behaviours its legacy code can trigger."""
    low = code.lower()
    picked = [t for pat, t in TRAPS if pat is None or re.search(pat, low)]
    return "\n".join(f"{i}. {t}" for i, t in enumerate(picked, 1))


SYSTEM = """You are a senior data engineer migrating SQL Server T-SQL and SAS jobs to PySpark on a lakehouse.
Output must reconcile with the legacy output record for record.

Rules for the code you write:
- PySpark DataFrame API or spark.sql on temp views; no pandas, no external calls, no printing.
- Expose exactly:  def run(spark, tables):  returning a dict {target_table_name: DataFrame}.
  `tables` maps each legacy INPUT table name (lower case, e.g. "dbo.claims") to a DataFrame.
- Import every pyspark function you use (e.g. from pyspark.sql import functions as F).
- Constants such as the T-SQL @asof date must be kept as in the legacy code.
Reply with JSON only."""

PROMPT = """Migrate legacy {lang} job {job_id}.

Legacy behaviours to reproduce exactly (a straight translation silently changes these):
{traps}
{lessons}
```
{code}
```

What it does: {summary}
Input tables (keys of `tables`): {inputs}
Column lineage:
{lineage}

Risks found during discovery:
{risks}

Target data contract(s) (table, primary key, columns; output must match):
{contracts}
{feedback}
Return JSON:
{reply_format}"""

REPLY_FIRST = """{"job_py": "<complete contents of job.py>",
  "notes": ["<at most 5 short lines: legacy behaviours you reproduced>"]}"""

# Repair: "diagnosis" comes first so the model commits to a cause before it writes the fix.
REPLY_REPAIR = """{"diagnosis": "<one or two sentences: the exact legacy behaviour that explains the differences, citing the input rows shown>",
  "change": "<one sentence: what you changed in the code because of it>",
  "job_py": "<complete contents of the corrected job.py>",
  "notes": ["<at most 5 short lines: legacy behaviours you reproduced>"]}"""

TEST_SYSTEM = """You write pytest unit tests for PySpark jobs migrated from legacy SQL. Reply with JSON only."""

TEST_PROMPT = """Write test_job.py for this migrated job {job_id} (legacy {lang}).

Legacy code:
```
{code}
```
Migrated job.py (verified against the legacy output):
```python
{job_py}
```
Legacy behaviours it must keep:
{traps}

2-4 small hand-made cases that exercise those behaviours. Use a local SparkSession
(pyspark.sql.SparkSession.builder.master("local[1]").getOrCreate()) and
`from migrated.{job_id}.job import run`.
Return JSON: {{"test_py": "<complete contents of test_job.py>"}}"""

FEEDBACK = """
THIS IS ATTEMPT {attempt}. The previous translation did NOT reconcile with the legacy output.
{history}
Previous job.py:
```python
{prev}
```
Reconciliation result (legacy vs migrated):
{diff}

Work like a debugger: for each sample, look at the input rows shown and work out why the LEGACY code
treats that row the way it does (check the behaviours listed above). State that cause in "diagnosis",
then change the code so it handles those rows the way legacy does. Keep everything that was right.
Returning the same code again is not accepted.
"""


# ============================================================== prompt building

def format_diff(report: ReconciliationReport) -> str:
    lines = []
    for t in report.tables:
        lines.append(f"table {t.table}: legacy rows {t.legacy_rows}, migrated rows {t.migrated_rows}")
        for col, diff in t.column_total_diffs.items():
            if diff:
                lines.append(f"  column total {col}: legacy - migrated = {diff}")
        if t.mismatched_keys:
            lines.append(f"  {t.mismatched_keys} rows differ by key")
        for s in t.sample_mismatches[:5]:
            lines.append(f"  sample: {json.dumps(s, default=str)}")
        if t.input_rows:
            lines.append("  INPUT rows behind those samples (values are quoted: look for NULLs, stray "
                         "spaces, letter case, duplicate keys):")
            lines += [f"    {json.dumps(r, default=str)}" for r in t.input_rows]
        lines += [f"  contract violation: {v}" for v in t.contract_violations]
    return "\n".join(lines) or "(no detail available)"


def build_prompt(job: LegacyJob, disc: DiscoveryResult, spec: DesignSpec, attempt: int = 1,
                 prev_code: Optional[str] = None, report: Optional[ReconciliationReport] = None,
                 history: Optional[list[str]] = None, lessons: str = "") -> str:
    """history: one line per failed attempt so far, oldest first.
    lessons: mistakes already made on earlier attempts and jobs (Lessons.for_prompt)."""
    if lessons:
        lessons = ("\nLessons from earlier attempts and jobs in this migration. These mistakes were "
                   "already made; do not repeat them:\n" + lessons + "\n")
    lineage = "\n".join(f"- {c.target_table}.{c.target_column} <- {', '.join(c.source_columns) or '(none)'}: "
                        f"{c.transformation}" for c in disc.column_lineage)
    contracts = "\n".join(
        f"- {c.table}  key={c.primary_key}\n" + "\n".join(
            f"    {x.name}: {x.dtype}{'' if x.nullable else ' NOT NULL'}{f'  check {x.rule}' if x.rule else ''}"
            for x in c.columns) for c in spec.contracts)
    feedback = ""
    if prev_code and report:
        earlier = ""
        if history:
            earlier = ("Attempts so far and how each failed (do not go back to code that failed):\n"
                       + "\n".join(f"- {h}" for h in history) + "\n")
        feedback = FEEDBACK.format(attempt=attempt, prev=prev_code, diff=format_diff(report), history=earlier)
    elif report:      # previous attempt produced no runnable code; say so (also avoids a cached replay)
        feedback = (f"\nTHIS IS ATTEMPT {attempt}. The previous attempt was rejected:\n{format_diff(report)}\n"
                    f"job_py must be plain Python source (no markdown fences) defining run(spark, tables).\n")
    return PROMPT.format(lang=job.language.value.upper(), job_id=job.job_id, code=job.code,
                         traps=traps_for(job.code), lessons=lessons,
                         summary=disc.summary or "(none)", inputs=", ".join(disc.input_tables),
                         lineage=lineage or "(none)",
                         risks="\n".join(f"- {r}" for r in disc.risk_notes) or "- none",
                         contracts=contracts, feedback=feedback,
                         reply_format=REPLY_REPAIR if prev_code and report else REPLY_FIRST)


# ============================================================== validation

_FENCE = re.compile(r"^\s*```[\w+-]*[ \t]*\n(.*?)\n?```\s*$", re.S)


def strip_fence(src: str) -> str:
    """Models often wrap the file in a markdown fence (```python ... ```) inside the JSON string."""
    m = _FENCE.match(src)
    return m.group(1).rstrip("\n") + "\n" if m else src


def check_files(job_py: str, test_py: Optional[str] = None) -> Optional[str]:
    """Return an error message, or None when the file(s) are acceptable."""
    for name, src in (("job_py", job_py), ("test_py", test_py)):
        if src is None:
            continue
        try:
            tree = ast.parse(src)
        except SyntaxError as e:
            return f"{name} has a syntax error: {e.msg} (line {e.lineno})"
        if name == "job_py":
            runs = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run"]
            if not runs or [a.arg for a in runs[0].args.args][:2] != ["spark", "tables"]:
                return "job_py must define top-level `def run(spark, tables)`"
    return None


def _shape(code: str) -> str:
    """Code with comments and layout removed, to tell whether two attempts are really the same."""
    try:
        return ast.dump(ast.parse(code))
    except SyntaxError:
        return code.strip()


# ============================================================== main

def generate(job: LegacyJob, disc: DiscoveryResult, spec: DesignSpec, llm: Optional[LLM] = None,
             attempt: int = 1, prev_code: Optional[str] = None,
             report: Optional[ReconciliationReport] = None,
             out_root: Optional[Path] = None, tried: Optional[list[str]] = None,
             history: Optional[list[str]] = None, lessons: str = "") -> GeneratedJob:
    """tried: code of every earlier failed attempt; a reply that repeats one of them is rejected.
    history: one line per earlier failed attempt, shown to the model."""
    llm = llm or get_llm()
    if not llm.available:
        raise RuntimeError("translation needs an LLM (set LLM_PROVIDER and an API key)")
    prompt = build_prompt(job, disc, spec, attempt, prev_code, report, history, lessons)
    seen = {_shape(c) for c in (tried or [])}
    notes: list[str] = []
    failures: list[str] = []
    for fix in range(MAX_FIX + 1):
        # a rejected reply is retried a little warmer, so the model does not send the same text again
        reply = llm.complete_json(SYSTEM, prompt, temperature=0.0 if fix == 0 else 0.4)
        job_py = strip_fence(str(reply.get("job_py", "")))
        notes = [f"{k}: {reply[k]}" for k in ("diagnosis", "change") if reply.get(k)]
        notes += [str(n) for n in reply.get("notes", [])][:5]
        error = check_files(job_py)
        if not error and _shape(job_py) in seen:
            error = ("job_py is the same code as an earlier attempt that already failed. Change the logic "
                     "that causes the reported differences")
        if not error:
            break
        failures.append(f"validation failure on try {fix + 1}: {error}")
        prompt += (f"\n\nYour last reply was rejected: {error}\nReturn the full corrected JSON; "
                   f"job_py holds plain Python source, no markdown fences.")
    else:
        raise ValueError(f"{job.job_id}: no valid translation after {MAX_FIX + 1} tries ({error})")

    notes = failures + notes
    root = out_root or ROOT / "migrated"
    d = root / job.job_id
    d.mkdir(parents=True, exist_ok=True)
    for pkg in (root / "__init__.py", d / "__init__.py"):
        pkg.touch()
    (d / "job.py").write_text(job_py)
    return GeneratedJob(job_id=job.job_id, attempt=attempt,
                        pyspark_path=str((d / "job.py").relative_to(root.parent)),
                        test_path=str((d / "test_job.py").relative_to(root.parent)),
                        translation_notes=notes, llm_provider=llm.provider, llm_model=llm.model)


def generate_tests(job: LegacyJob, llm: LLM, out_root: Optional[Path] = None) -> Optional[str]:
    """Write migrated/<job>/test_job.py for a job whose code has passed. Done once per job, not on
    every attempt: the test file is over half of a translation reply, and replies are the slow part
    on a local model. Returns an error message, or None."""
    d = (out_root or ROOT / "migrated") / job.job_id
    job_py = (d / "job.py").read_text()
    prompt = TEST_PROMPT.format(job_id=job.job_id, lang=job.language.value.upper(), code=job.code,
                                job_py=job_py, traps=traps_for(job.code))
    reply = llm.complete_json(TEST_SYSTEM, prompt, temperature=0.0)
    test_py = strip_fence(str(reply.get("test_py", "")))
    error = check_files(job_py, test_py) if test_py.strip() else "empty test file"
    if not error:
        (d / "test_job.py").write_text(test_py)
    return error


def main() -> None:
    from factory.agents.design import DesignResult
    from factory.agents.discovery import load_jobs

    load_env()
    run_dir = ROOT / sys.argv[1]
    llm = get_llm()
    only = set(sys.argv[2:])
    out = run_dir / "generate"
    out.mkdir(exist_ok=True)
    for job in load_jobs():
        if only and job.job_id[:3] not in only:
            continue
        disc = DiscoveryResult.model_validate_json((run_dir / "discovery" / f"{job.job_id}.json").read_text())
        spec = DesignResult.model_validate_json((run_dir / "design" / f"{job.job_id}.json").read_text()).spec
        g = generate(job, disc, spec, llm)
        (out / f"{job.job_id}.json").write_text(g.model_dump_json(indent=2))
        print(f"  {job.job_id:34s} attempt {g.attempt}  {g.pyspark_path}  ({len(g.translation_notes)} notes)")


if __name__ == "__main__":
    sys.exit(main())
