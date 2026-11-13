"""
Builds databricks/02_run_silver_gold.py (a Databricks notebook) from reference_jobs/jobs.py,
so the notebook and the tested module never drift apart.

    python databricks/build_notebook.py
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JOBS = (ROOT / "reference_jobs" / "jobs.py").read_text()
JOBS = JOBS.replace("from __future__ import annotations\n", "")
SEP = "\n\n# COMMAND ----------\n\n"


def md(text: str) -> str:
    return "\n".join("# MAGIC " + line if line else "# MAGIC" for line in ["%md", *text.strip().splitlines()])


HEADER = md("""
# 02 · Silver and gold: run the migrated jobs

Runs the PySpark translations of the 12 legacy jobs (9 T-SQL, 3 SAS) in lineage order and checks
each output against the legacy golden output, record for record.

| Wave | Jobs | Writes |
|---|---|---|
| 1 | J01, J02, J03, J04 (SAS), J05 | `silver.customers`, `policies`, `premiums`, `claim_payments`, `claims` |
| 2 | J06, J07 (SAS), J08, J09, J11, J12 | `silver.policy_earned_premium`, `claim_totals`; `gold.monthly_claims_paid`, `premium_collections`, `broker_commission`, `open_reserves` |
| 3 | J10 (SAS) | `gold.loss_ratio` |

**Before running:** notebook 01 has built the bronze tables, and the 12 golden CSVs from
`legacy/golden/` are uploaded to `/Volumes/workspace/legacy_golden/files` (for the check).

**Widgets:** `job_id` = `ALL` or one job (`J05`) to run a single job, which is how the Databricks
Job (`databricks/migration_job.json`) runs one task per legacy job.

Generated from `reference_jobs/jobs.py` by `databricks/build_notebook.py`. Edit the module, not this notebook.
""")

CONFIG = '''dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("job_id", "ALL", "Job to run (ALL or J01..J12)")
dbutils.widgets.dropdown("check_golden", "yes", ["yes", "no"], "Compare with legacy golden output")
CATALOG = dbutils.widgets.get("catalog")
JOB_ID = dbutils.widgets.get("job_id").strip().upper()
CHECK = dbutils.widgets.get("check_golden") == "yes"
GOLDEN_PATH = f"/Volumes/{CATALOG}/legacy_golden/files"
for layer in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{layer}")'''

RUN = '''def read_table(name):
    return spark.table(f"{CATALOG}.{name}")


def write_table(df, name):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{name}"))


only = None if JOB_ID == "ALL" else {JOB_ID}
if only and JOB_ID not in {j.job_id for j in JOBS}:
    raise ValueError(f"Unknown job_id {JOB_ID}; use ALL or one of {[j.job_id for j in JOBS]}")
counts = run_pipeline(spark, read_table, write_table, only)'''

CHECK_MD = md("""
## Check against the legacy output
Each migrated table is compared with the CSV the legacy job produced (SQL Server for T-SQL,
SAS OnDemand for SAS). Golden values are read as text and cast to the migrated column types, so
`1000.005` from SAS and `1000.005` from Spark compare exactly. A table **matches** when the row
counts agree and no row is missing from, or extra in, either side.
""")

CHECK_CODE = '''from pyspark.sql import functions as F

def compare(job, table, golden_name):
    mig = spark.table(f"{CATALOG}.{table}")
    path = f"{GOLDEN_PATH}/{golden_name}.csv"
    try:
        gold = spark.read.option("header", True).csv(path)
    except Exception:
        return (job.job_id, table, None, mig.count(), None, f"no golden file at {path}")
    gold = gold.toDF(*[c.lower() for c in gold.columns])
    types = dict(mig.dtypes)
    cols = [c for c in mig.columns if c in gold.columns]
    def cast(c):
        v = F.trim(F.col(c)) if types[c] != "string" else F.col(c)
        return F.when(v == "", None).otherwise(v).cast(types[c]).alias(c)
    g, m = gold.select(*[cast(c) for c in cols]), mig.select(*cols)
    missing, extra = g.exceptAll(m).count(), m.exceptAll(g).count()
    gn, mn = g.count(), m.count()
    absent = [c for c in mig.columns if c not in gold.columns]
    ok = gn == mn and missing == 0 and extra == 0 and not absent
    detail = "MATCHES LEGACY" if ok else (f"{missing} legacy rows not reproduced, {extra} extra rows"
                                          + (f"; columns not in golden file: {absent}" if absent else ""))
    return (job.job_id, table, gn, mn, ok, detail)

results = []
if CHECK:
    for job in sorted(JOBS, key=lambda j: (j.wave, j.job_id)):
        if only and job.job_id not in only:
            continue
        for table, golden_name in zip(job.outputs, job.golden):
            results.append(compare(job, table, golden_name))
    display(spark.createDataFrame(results, "job string, table string, legacy_rows long, "
                                           "migrated_rows long, matches boolean, detail string"))
    bad = [r for r in results if r[4] is False]
    if bad:
        raise AssertionError(f"{len(bad)} table(s) differ from legacy: {[r[1] for r in bad]}")'''

DIFF_MD = md("""
## Investigate a mismatch
Set `table` and `golden` below to see rows that differ (only needed if the check above fails).
""")

DIFF_CODE = '''table, golden = "silver.claims", "stg_claims"    # e.g. "gold.loss_ratio", "rpt_loss_ratio"
mig = spark.table(f"{CATALOG}.{table}")
types = dict(mig.dtypes)
g = spark.read.option("header", True).csv(f"{GOLDEN_PATH}/{golden}.csv")
g = g.select(*[F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c)).cast(types[c.lower()]).alias(c.lower())
               for c in g.columns if c.lower() in types])
m = mig.select(*g.columns)
display(g.exceptAll(m).withColumn("side", F.lit("legacy only"))
         .unionByName(m.exceptAll(g).withColumn("side", F.lit("migrated only"))).limit(50))'''

def job_definition(notebook_path: str = "/Workspace/Users/<your-email>/02_run_silver_gold") -> dict:
    """Databricks Job: one task per legacy job, depends_on taken from the lineage
    (B depends on A when B reads a table A writes). Serverless: no cluster settings."""
    import sys
    sys.path.insert(0, str(ROOT))
    from reference_jobs.jobs import JOBS, LEGACY_TO_LAKEHOUSE
    writer = {t: j.job_id for j in JOBS for t in j.outputs}
    tasks = []
    for j in sorted(JOBS, key=lambda j: (j.wave, j.job_id)):
        deps = sorted({writer[LEGACY_TO_LAKEHOUSE[i]] for i in j.inputs if LEGACY_TO_LAKEHOUSE[i] in writer})
        task = {"task_key": j.name,
                "description": f"Wave {j.wave}: legacy {j.language.upper()} job {j.job_id} -> {', '.join(j.outputs)}",
                "notebook_task": {"notebook_path": notebook_path, "source": "WORKSPACE",
                                  "base_parameters": {"job_id": j.job_id, "check_golden": "yes"}}}
        if deps:
            task["depends_on"] = [{"task_key": next(x.name for x in JOBS if x.job_id == d)} for d in deps]
        tasks.append(task)
    return {"name": "EXL migration factory - silver and gold", "max_concurrent_runs": 1,
            "tags": {"project": "exl-hackathon-challenge-03"}, "tasks": tasks}


import json  # noqa: E402
(ROOT / "databricks" / "migration_job.json").write_text(json.dumps(job_definition(), indent=2) + "\n")
print("wrote databricks/migration_job.json")

notebook = "# Databricks notebook source\n" + SEP.join([HEADER, CONFIG, JOBS.strip(), RUN, CHECK_MD, CHECK_CODE, DIFF_MD, DIFF_CODE]) + "\n"
out = ROOT / "databricks" / "02_run_silver_gold.py"
out.write_text(notebook)
print(f"wrote {out.relative_to(ROOT)} ({len(notebook.splitlines())} lines)")
