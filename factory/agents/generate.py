"""
Translation agent: legacy code + lineage + design in, PySpark job + unit tests out.

Every generated job has the same shape, so the reconciler and the tests can call it:

    def run(spark, tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
        # tables: legacy input tables by name, e.g. "dbo.claims", "stg.policies"
        # returns: target tables by their new name, e.g. "gold.open_reserves"

Safety nets around the LLM:
  * the reply must be JSON with `job_py` and `test_py`
  * both files must parse (ast) and job_py must define run(spark, tables)
  * if not, the error is fed back and the model retries (up to MAX_FIX times)
  * repair mode: pass the previous code and a ReconciliationReport; the diff is
    turned into a targeted prompt (used by the self-fix loop)

    python -m factory.agents.generate runs/<run_id>      # needs discovery/ and design/
"""
from __future__ import annotations

import ast
import json
import sys
from pathlib import Path
from typing import Optional

from factory.config import ROOT, load_env
from factory.contracts.models import (DesignSpec, DiscoveryResult, GeneratedJob, LegacyJob,
                                      ReconciliationReport)
from factory.llm.base import LLM, get_llm

MAX_FIX = 2          # syntax / shape retries inside one attempt

TRAPS = """Behaviours where a straight translation silently changes results. Reproduce the LEGACY behaviour exactly:
1. SQL Server compares strings case-insensitively and ignores TRAILING spaces; leading spaces still matter.
   `status = 'OPEN'` / `<> 'REJECTED'` therefore match 'open' and 'OPEN  ' but not ' Open'.
   In Spark use upper(rtrim(col)) on both sides. Keep untrimmed values in output columns unless the legacy code trims them.
2. NULL: in T-SQL a comparison with NULL is unknown, so the row fails WHERE and falls to ELSE in CASE.
   SAS treats a missing number as lower than every number (`if amount < 100` is TRUE for missing).
   SAS `a + b` is missing if either is missing; SAS sum() ignores missing. Do not add coalesce(...,0) unless the legacy code does.
3. COUNT(*) counts rows with NULLs; COUNT(col) skips NULLs; COUNT(DISTINCT col) skips NULLs.
4. DISTINCT / SAS `proc sort nodupkey` remove duplicates BEFORE aggregating. Keep that order. nodupkey keeps one row per key.
5. DATEDIFF(YEAR, a, b) counts calendar-year boundaries (year(b) - year(a)), NOT elapsed years. DATEDIFF(DAY,a,b) = datediff(b, a).
6. ROUND(x, 2) in T-SQL and SAS rounds half AWAY from zero. Use Spark round() on DECIMAL columns, never bround(), Python round() or float.
   Decimal arithmetic: keep decimals; `x * n / 365.0` in T-SQL is decimal, so cast to decimal rather than double.
7. COALESCE/ISNULL defaults (e.g. end_date -> 9999-12-31) must be applied exactly where the legacy code applies them.
8. GROUP BY on a NULL key forms its own group in both T-SQL and SAS; keep it (do not drop NULL-key rows). FULL JOIN keeps unmatched rows from both sides.
9. Output column names, order and types must match the data contract exactly."""

SYSTEM = f"""You are a senior data engineer migrating SQL Server T-SQL and SAS jobs to PySpark on a lakehouse.
Output must reconcile with the legacy output record for record.

{TRAPS}

Rules for the code you write:
- PySpark DataFrame API or spark.sql on temp views; no pandas, no external calls, no printing.
- Expose exactly:  def run(spark, tables):  returning a dict {{target_table_name: DataFrame}}.
  `tables` maps each legacy INPUT table name (lower case, e.g. "dbo.claims") to a DataFrame.
- Constants such as the T-SQL @asof date must be kept as in the legacy code.
- The test file is a pytest file with 2-4 small hand-made cases (local SparkSession via
  pyspark.sql.SparkSession.builder.master("local[1]").getOrCreate()) that exercise the traps above
  for this job. Import with: from migrated.<job_id>.job import run
Reply with JSON only."""

PROMPT = """Migrate legacy {lang} job {job_id}.

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
{{"job_py": "<complete contents of job.py>",
  "test_py": "<complete contents of test_job.py>",
  "notes": ["<each assumption or trap you handled, one line each>"]}}"""

FEEDBACK = """
THIS IS ATTEMPT {attempt}. The previous translation did NOT reconcile with the legacy output.

Previous job.py:
```python
{prev}
```
Reconciliation result (legacy vs migrated):
{diff}

Find the legacy behaviour the previous code got wrong (check the trap list), fix it, and keep everything that was right.
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
        lines += [f"  contract violation: {v}" for v in t.contract_violations]
    return "\n".join(lines) or "(no detail available)"


def build_prompt(job: LegacyJob, disc: DiscoveryResult, spec: DesignSpec, attempt: int = 1,
                 prev_code: Optional[str] = None, report: Optional[ReconciliationReport] = None) -> str:
    lineage = "\n".join(f"- {c.target_table}.{c.target_column} <- {', '.join(c.source_columns) or '(none)'}: "
                        f"{c.transformation}" for c in disc.column_lineage)
    contracts = "\n".join(
        f"- {c.table}  key={c.primary_key}\n" + "\n".join(
            f"    {x.name}: {x.dtype}{'' if x.nullable else ' NOT NULL'}{f'  check {x.rule}' if x.rule else ''}"
            for x in c.columns) for c in spec.contracts)
    feedback = ""
    if prev_code and report:
        feedback = FEEDBACK.format(attempt=attempt, prev=prev_code, diff=format_diff(report))
    return PROMPT.format(lang=job.language.value.upper(), job_id=job.job_id, code=job.code,
                         summary=disc.summary or "(none)", inputs=", ".join(disc.input_tables),
                         lineage=lineage or "(none)",
                         risks="\n".join(f"- {r}" for r in disc.risk_notes) or "- none",
                         contracts=contracts, feedback=feedback)


# ============================================================== validation

def check_files(job_py: str, test_py: str) -> Optional[str]:
    """Return an error message, or None when both files are acceptable."""
    for name, src in (("job_py", job_py), ("test_py", test_py)):
        try:
            tree = ast.parse(src)
        except SyntaxError as e:
            return f"{name} has a syntax error: {e.msg} (line {e.lineno})"
        if name == "job_py":
            runs = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run"]
            if not runs or [a.arg for a in runs[0].args.args][:2] != ["spark", "tables"]:
                return "job_py must define top-level `def run(spark, tables)`"
    return None


# ============================================================== main

def generate(job: LegacyJob, disc: DiscoveryResult, spec: DesignSpec, llm: Optional[LLM] = None,
             attempt: int = 1, prev_code: Optional[str] = None,
             report: Optional[ReconciliationReport] = None,
             out_root: Optional[Path] = None) -> GeneratedJob:
    llm = llm or get_llm()
    if not llm.available:
        raise RuntimeError("translation needs an LLM (set LLM_PROVIDER and an API key)")
    prompt = build_prompt(job, disc, spec, attempt, prev_code, report)
    notes: list[str] = []
    failures: list[str] = []
    for fix in range(MAX_FIX + 1):
        reply = llm.complete_json(SYSTEM, prompt, temperature=0.0)
        job_py, test_py = str(reply.get("job_py", "")), str(reply.get("test_py", ""))
        notes = [str(n) for n in reply.get("notes", [])]
        error = check_files(job_py, test_py)
        if not error:
            break
        failures.append(f"validation failure on try {fix + 1}: {error}")
        prompt += f"\n\nYour last reply was rejected: {error}\nReturn the full corrected JSON."
    else:
        raise ValueError(f"{job.job_id}: no valid translation after {MAX_FIX + 1} tries ({error})")

    notes = failures + notes
    root = out_root or ROOT / "migrated"
    d = root / job.job_id
    d.mkdir(parents=True, exist_ok=True)
    for pkg in (root / "__init__.py", d / "__init__.py"):
        pkg.touch()
    (d / "job.py").write_text(job_py)
    (d / "test_job.py").write_text(test_py)
    return GeneratedJob(job_id=job.job_id, attempt=attempt,
                        pyspark_path=str((d / "job.py").relative_to(root.parent)),
                        test_path=str((d / "test_job.py").relative_to(root.parent)),
                        translation_notes=notes, llm_provider=llm.provider, llm_model=llm.model)


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
