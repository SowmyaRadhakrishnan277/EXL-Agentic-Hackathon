# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver and gold: run the migrated jobs
# MAGIC
# MAGIC Runs the PySpark translations of the 12 legacy jobs (9 T-SQL, 3 SAS) in lineage order and checks
# MAGIC each output against the legacy golden output, record for record.
# MAGIC
# MAGIC | Wave | Jobs | Writes |
# MAGIC |---|---|---|
# MAGIC | 1 | J01, J02, J03, J04 (SAS), J05 | `silver.customers`, `policies`, `premiums`, `claim_payments`, `claims` |
# MAGIC | 2 | J06, J07 (SAS), J08, J09, J11, J12 | `silver.policy_earned_premium`, `claim_totals`; `gold.monthly_claims_paid`, `premium_collections`, `broker_commission`, `open_reserves` |
# MAGIC | 3 | J10 (SAS) | `gold.loss_ratio` |
# MAGIC
# MAGIC **Before running:** notebook 01 has built the bronze tables, and the 12 golden CSVs from
# MAGIC `legacy/golden/` are uploaded to `/Volumes/workspace/legacy_golden/files` (for the check).
# MAGIC
# MAGIC **Widgets:** `job_id` = `ALL` or one job (`J05`) to run a single job, which is how the Databricks
# MAGIC Job (`databricks/migration_job.json`) runs one task per legacy job.
# MAGIC
# MAGIC Generated from `reference_jobs/jobs.py` by `databricks/build_notebook.py`. Edit the module, not this notebook.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("job_id", "ALL", "Job to run (ALL or J01..J12)")
dbutils.widgets.dropdown("check_golden", "yes", ["yes", "no"], "Compare with legacy golden output")
CATALOG = dbutils.widgets.get("catalog")
JOB_ID = dbutils.widgets.get("job_id").strip().upper()
CHECK = dbutils.widgets.get("check_golden") == "yes"
GOLDEN_PATH = f"/Volumes/{CATALOG}/legacy_golden/files"
for layer in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{layer}")

# COMMAND ----------

"""
Reference PySpark translations of the 12 legacy jobs (9 T-SQL, 3 SAS).

Hand-written baseline, kept separate from the agents' output in migrated/:
  * shows what a correct migration looks like, trap by trap
  * gives the demo a known-good silver/gold build on Databricks
  * lets the agents' code be compared with a human translation

Every job has the factory's shape, so the reconciler can check it:

    def run(spark, tables: dict[str, DataFrame]) -> dict[str, DataFrame]
        tables: inputs by LEGACY name ("dbo.claims", "stg.policies", "legacy.claim_totals")
        returns: outputs by LAKEHOUSE name ("silver.claims", "gold.loss_ratio")

Legacy behaviour reproduced on purpose (see docs/legacy_jobs_answer_key.md):
  T-SQL `=` / `<>` on text ignores trailing spaces and case (default CI collation),
        but NOT leading spaces                      -> upper(rtrim(x)) compared
  T-SQL `x <> 'A'` is unknown when x is NULL        -> row excluded (same in Spark)
  T-SQL DATEDIFF(YEAR, a, b) counts year boundaries -> year(b) - year(a), not true age
  T-SQL / SAS ROUND: half away from zero            -> decimal arithmetic + F.round (HALF_UP)
  SAS   missing numeric < any number is TRUE        -> explicit isNull() branches
  SAS   a + b is missing if either is missing       -> plain decimal +, no coalesce
  SAS   char comparison ignores trailing blanks, case-sensitive -> rtrim(x) == 'A'
  SAS   column order = order variables first appear in the DATA step

This module only imports pyspark, so it runs locally, through Databricks Connect,
or pasted into a Databricks notebook (databricks/02_run_silver_gold.py is built from it).
"""

from dataclasses import dataclass
from decimal import Decimal

from pyspark.sql import DataFrame, functions as F

ASOF_AGE = "2025-06-30"      # J01: actuarial age reporting date
ASOF_CLAIMS = "2025-12-31"   # J05: claim ageing date
POLICY_YEARS = [2021, 2022, 2023, 2024, 2025]
OPEN_END = "9999-12-31"


def tsql_text_eq(col, value: str):
    """T-SQL `col = 'VALUE'` under SQL_Latin1_General_CP1_CI_AS: trailing spaces and case ignored."""
    return F.upper(F.rtrim(col)) == value


def sas_text_eq(col, value: str):
    """SAS `col = 'VALUE'`: trailing blanks ignored, case-sensitive."""
    return F.rtrim(col) == value


# ------------------------------------------------------------------ wave 1

def j01_stg_customers(spark, t):
    c = t["dbo.customers"]
    out = c.select(
        "customer_id",
        F.upper(F.trim("county")).alias("county"),
        "date_of_birth",
        # DATEDIFF(YEAR, dob, @asof) counts year boundaries crossed, so someone born
        # 1990-12-01 is 35 on 2025-06-30. Reproduce that, not their true age (34).
        (F.year(F.lit(ASOF_AGE).cast("date")) - F.year("date_of_birth")).cast("int").alias("age_years"),
        "customer_since",
        "segment",
    )
    return {"silver.customers": out}


def j02_stg_policies(spark, t):
    p, pr = t["dbo.policies"].alias("p"), t["dbo.products"].alias("pr")
    out = p.join(pr, F.col("pr.product_code") == F.col("p.product_code"), "inner").select(
        "p.policy_id",
        "p.customer_id",
        "p.product_code",
        "pr.line_of_business",
        "p.broker_id",
        "p.channel",
        "p.start_date",
        F.coalesce("p.end_date", F.lit(OPEN_END).cast("date")).alias("end_date"),
        F.when(F.col("p.end_date").isNull(), "Y").otherwise("N").alias("rolling_flag"),
        F.upper(F.trim("p.status")).alias("status"),
        "p.annual_premium",
        "p.payment_frequency",
    )
    return {"silver.policies": out}


def j03_stg_premiums(spark, t):
    pm = t["dbo.premiums"]
    out = pm.select(
        "premium_id", "policy_id", "due_date", "paid_date", "amount",
        F.when(F.col("paid_date").isNull(), "N").otherwise("Y").alias("paid_flag"),
    ).distinct()                                   # SELECT DISTINCT: drops the exact repeat rows
    return {"silver.premiums": out}


def j04_stg_claim_payments(spark, t):
    cp = t["dbo.claim_payments"]
    # proc sort nodupkey by payment_id. The repeats are exact copies, so which one is kept
    # does not matter.
    dedup = cp.dropDuplicates(["payment_id"])
    amount = F.col("amount")
    out = dedup.select(
        # SAS variable order: the LENGTH statement (payment_id, claim_id, payment_type) comes
        # before the INFORMAT (payment_date), then amount, then amount_band.
        "payment_id", "claim_id", "payment_type", "payment_date", "amount",
        # SAS: a missing amount is lower than any number, so `amount < 100` is TRUE -> 'SMALL'.
        F.when(amount.isNull() | (amount < 100), "SMALL")
         .when(amount < 1000, "MEDIUM")
         .otherwise("LARGE").alias("amount_band"),
    )
    return {"silver.claim_payments": out}


def j05_stg_claims(spark, t):
    c = t["dbo.claims"]
    # WHERE status <> 'REJECTED': 'rejected' and 'REJECTED  ' are excluded (CI collation,
    # trailing spaces ignored), ' Rejected' is kept (leading space), NULL status is excluded.
    out = c.filter(~tsql_text_eq(F.col("status"), "REJECTED")).select(
        "claim_id", "policy_id", "loss_date", "reported_date", "claim_type",
        "status",                                  # passed through unchanged
        "reserve_amount",
        F.datediff(F.lit(ASOF_CLAIMS).cast("date"), F.col("reported_date")).cast("int").alias("claim_age_days"),
    )
    return {"silver.claims": out}


# ------------------------------------------------------------------ wave 2

def j06_stg_policy_earned_premium(spark, t):
    p = t["stg.policies"]
    yr = F.col("policy_year")
    jan1, dec31 = F.make_date(yr, F.lit(1), F.lit(1)), F.make_date(yr, F.lit(12), F.lit(31))
    y = p.withColumn("policy_year", F.explode(F.array(*[F.lit(v) for v in POLICY_YEARS])))
    y = y.filter((F.col("start_date") <= dec31) & (F.col("end_date") >= jan1))
    days = F.datediff(F.least("end_date", dec31), F.greatest("start_date", jan1)) + 1
    y = y.withColumn("days_in_force", days.cast("int"))
    out = y.select(
        "policy_id", "product_code", F.col("policy_year").cast("int").alias("policy_year"),
        "days_in_force",
        # decimal(18,2) * int / 365.0, rounded half away from zero; no float maths.
        F.round(F.col("annual_premium") * F.col("days_in_force") / F.lit(Decimal("365.0")), 2)
         .alias("earned_premium"),
    )
    return {"silver.policy_earned_premium": out}


def j07_stg_claim_totals(spark, t):
    pay, claims = t["legacy.stg_claim_payments"], t["stg.claims"]
    by_claim = pay.groupBy("claim_id").agg(
        F.sum(F.when(sas_text_eq(F.col("payment_type"), "INDEMNITY"), F.col("amount"))).alias("indemnity_paid"),
        F.sum(F.when(sas_text_eq(F.col("payment_type"), "EXPENSE"), F.col("amount"))).alias("expense_paid"),
        F.count(F.lit(1)).alias("payment_count"),
    )
    j = claims.join(by_claim, "claim_id", "left")
    # SAS: `indemnity + expense` is missing when either side is missing (claims with no
    # expense rows), and missing < 500 is TRUE, so those claims get flag 'Y'.
    total = F.round(F.col("indemnity_paid") + F.col("expense_paid"), 2)
    out = j.select(
        "claim_id", "policy_id", "claim_type", "loss_date", "indemnity_paid", "expense_paid",
        F.coalesce("payment_count", F.lit(0)).cast("bigint").alias("payment_count"),
        # LENGTH small_claim_flag comes before total_paid is assigned, so it is the earlier column.
        F.when(total.isNull() | (total < 500), "Y").otherwise("N").alias("small_claim_flag"),
        total.alias("total_paid"),
    )
    return {"silver.claim_totals": out}


def j08_rpt_monthly_claims_paid(spark, t):
    cp = (t["dbo.claim_payments"]
          .select("payment_id", "claim_id", "payment_date", "payment_type", "amount")
          .distinct()                              # inline DISTINCT removes repeat payments
          .alias("cp"))
    c, p = t["stg.claims"].alias("c"), t["stg.policies"].alias("p")
    j = (cp.join(c, F.col("c.claim_id") == F.col("cp.claim_id"))
           .join(p, F.col("p.policy_id") == F.col("c.policy_id"))
           .filter(tsql_text_eq(F.col("cp.payment_type"), "INDEMNITY") & F.col("cp.payment_date").isNotNull()))
    out = j.groupBy(F.col("p.product_code").alias("product_code"),
                    F.trunc("cp.payment_date", "month").alias("payment_month")).agg(
        F.countDistinct("cp.claim_id").alias("claims_paid"),
        F.count(F.lit(1)).alias("payments"),
        F.round(F.sum("cp.amount"), 2).alias("indemnity_paid"),
    )
    return {"gold.monthly_claims_paid": out}


def j09_rpt_premium_collections(spark, t):
    pm = t["stg.premiums"]
    collected = F.sum(F.when(F.col("paid_flag") == "Y", F.col("amount")).otherwise(F.lit(0)))
    due = F.sum("amount")
    g = pm.groupBy(F.trunc("due_date", "month").alias("due_month")).agg(
        F.count(F.lit(1)).alias("instalments_due"),   # COUNT(*): NULL-amount instalments still count
        due.alias("amount_due"),
        collected.alias("amount_collected"),
    )
    out = g.select(
        "due_month", "instalments_due", "amount_due", "amount_collected",
        (F.col("amount_due") - F.col("amount_collected")).alias("arrears"),
        (F.lit(Decimal("100.0")) * F.col("amount_collected")
         / F.when(F.col("amount_due") != 0, F.col("amount_due"))).cast("decimal(9,2)").alias("collection_rate_pct"),
    )
    return {"gold.premium_collections": out}


def j11_rpt_broker_commission(spark, t):
    p, b = t["stg.policies"].alias("p"), t["dbo.brokers"].alias("b")
    j = (p.join(b, F.col("b.broker_id") == F.col("p.broker_id"))
          .filter(tsql_text_eq(F.col("b.active_flag"), "Y")
                  & (F.col("p.status") != "CANCELLED")     # NULL status is excluded, as in T-SQL
                  & F.year("p.start_date").between(2021, 2025)))
    out = j.groupBy(F.col("b.broker_id").alias("broker_id"),
                    F.year("p.start_date").cast("int").alias("written_year")).agg(
        F.count(F.lit(1)).alias("policies_written"),
        F.sum("p.annual_premium").alias("written_premium"),
        F.round(F.sum(F.col("p.annual_premium") * F.col("b.commission_rate")), 2).alias("commission_due"),
    )
    return {"gold.broker_commission": out}


def j12_rpt_open_reserves(spark, t):
    c, p = t["stg.claims"].alias("c"), t["stg.policies"].alias("p")
    j = c.join(p, F.col("p.policy_id") == F.col("c.policy_id")).filter(
        tsql_text_eq(F.col("c.status"), "OPEN"))      # matches 'open', 'OPEN  '; not ' Open'
    age = F.col("c.claim_age_days")
    band = (F.when(age <= 30, "0-30").when(age <= 90, "31-90").when(age <= 180, "91-180")
             .otherwise("180+"))                        # NULL age falls to ELSE, as in T-SQL
    out = j.groupBy(F.col("p.product_code").alias("product_code"), band.alias("age_band")).agg(
        F.count(F.lit(1)).alias("open_claims"),
        F.count("c.reserve_amount").alias("claims_with_reserve"),
        F.sum("c.reserve_amount").alias("total_reserve"),
    )
    return {"gold.open_reserves": out}


# ------------------------------------------------------------------ wave 3

def j10_rpt_loss_ratio(spark, t):
    ct, pol, epp = t["legacy.claim_totals"], t["stg.policies"], t["stg.policy_earned_premium"]
    earned = epp.groupBy("product_code", "policy_year").agg(F.sum("earned_premium").alias("earned_premium")).alias("e")
    incurred = (ct.join(pol.select("policy_id", "product_code"), "policy_id")
                  .groupBy("product_code", F.year("loss_date").alias("loss_year"))   # NULL loss_date -> own group
                  .agg(F.sum("indemnity_paid").alias("incurred"), F.count(F.lit(1)).alias("claim_count"))
                  .alias("i"))
    j = earned.join(incurred, (F.col("e.product_code") == F.col("i.product_code"))
                    & (F.col("e.policy_year") == F.col("i.loss_year")), "full_outer")
    ep, inc = F.col("e.earned_premium"), F.col("i.incurred")
    out = j.select(
        F.coalesce("e.product_code", "i.product_code").alias("product_code"),
        F.coalesce("e.policy_year", "i.loss_year").cast("int").alias("year"),
        ep.alias("earned_premium"),
        inc.alias("incurred"),
        F.col("i.claim_count").alias("claim_count"),
        F.when(ep > 0, F.round(inc / ep, 4)).alias("loss_ratio"),
    )
    return {"gold.loss_ratio": out}


# ------------------------------------------------------------------ registry and runner

@dataclass(frozen=True)
class Job:
    job_id: str
    name: str
    language: str
    wave: int
    run: object
    inputs: tuple[str, ...]          # legacy names
    outputs: tuple[str, ...]         # lakehouse names
    golden: tuple[str, ...]          # legacy/golden/<name>.csv, one per output


JOBS = [
    Job("J01", "J01_stg_customers", "tsql", 1, j01_stg_customers, ("dbo.customers",), ("silver.customers",), ("stg_customers",)),
    Job("J02", "J02_stg_policies", "tsql", 1, j02_stg_policies, ("dbo.policies", "dbo.products"), ("silver.policies",), ("stg_policies",)),
    Job("J03", "J03_stg_premiums", "tsql", 1, j03_stg_premiums, ("dbo.premiums",), ("silver.premiums",), ("stg_premiums",)),
    Job("J04", "J04_stg_claim_payments", "sas", 1, j04_stg_claim_payments, ("dbo.claim_payments",), ("silver.claim_payments",), ("stg_claim_payments",)),
    Job("J05", "J05_stg_claims", "tsql", 1, j05_stg_claims, ("dbo.claims",), ("silver.claims",), ("stg_claims",)),
    Job("J06", "J06_stg_policy_earned_premium", "tsql", 2, j06_stg_policy_earned_premium, ("stg.policies",), ("silver.policy_earned_premium",), ("stg_policy_earned_premium",)),
    Job("J07", "J07_stg_claim_totals", "sas", 2, j07_stg_claim_totals, ("legacy.stg_claim_payments", "stg.claims"), ("silver.claim_totals",), ("claim_totals",)),
    Job("J08", "J08_rpt_monthly_claims_paid", "tsql", 2, j08_rpt_monthly_claims_paid, ("dbo.claim_payments", "stg.claims", "stg.policies"), ("gold.monthly_claims_paid",), ("rpt_monthly_claims_paid",)),
    Job("J09", "J09_rpt_premium_collections", "tsql", 2, j09_rpt_premium_collections, ("stg.premiums",), ("gold.premium_collections",), ("rpt_premium_collections",)),
    Job("J11", "J11_rpt_broker_commission", "tsql", 2, j11_rpt_broker_commission, ("stg.policies", "dbo.brokers"), ("gold.broker_commission",), ("rpt_broker_commission",)),
    Job("J12", "J12_rpt_open_reserves", "tsql", 2, j12_rpt_open_reserves, ("stg.claims", "stg.policies"), ("gold.open_reserves",), ("rpt_open_reserves",)),
    Job("J10", "J10_rpt_loss_ratio", "sas", 3, j10_rpt_loss_ratio, ("legacy.claim_totals", "stg.policies", "stg.policy_earned_premium"), ("gold.loss_ratio",), ("rpt_loss_ratio",)),
]

# Same rules as factory/catalog.py, repeated here so the module stands alone in a notebook.
LEGACY_TO_LAKEHOUSE = {
    "dbo.customers": "bronze.customers", "dbo.policies": "bronze.policies", "dbo.products": "bronze.products",
    "dbo.premiums": "bronze.premiums", "dbo.claims": "bronze.claims", "dbo.claim_payments": "bronze.claim_payments",
    "dbo.brokers": "bronze.brokers",
    "stg.policies": "silver.policies", "stg.claims": "silver.claims", "stg.premiums": "silver.premiums",
    "stg.policy_earned_premium": "silver.policy_earned_premium",
    "legacy.stg_claim_payments": "silver.claim_payments", "legacy.claim_totals": "silver.claim_totals",
}


def run_pipeline(spark, read_table, write_table, only=None, log=print) -> dict[str, dict[str, int]]:
    """Run the jobs in wave order. read_table(lakehouse_name) -> DataFrame,
    write_table(df, lakehouse_name). Returns {job_id: {table: rows}}."""
    counts = {}
    for job in sorted(JOBS, key=lambda j: (j.wave, j.job_id)):
        if only and job.job_id not in only:
            continue
        inputs = {}
        for legacy in job.inputs:
            df = read_table(LEGACY_TO_LAKEHOUSE[legacy])
            inputs[legacy] = df.drop(*[c for c in df.columns if c.startswith("_")])   # bronze audit cols
        outputs = job.run(spark, inputs)
        counts[job.job_id] = {}
        for table in job.outputs:
            write_table(outputs[table], table)
            counts[job.job_id][table] = read_table(table).count()
            log(f"wave {job.wave}  {job.name:32s} -> {table:30s} {counts[job.job_id][table]:>9,} rows")
    return counts

# COMMAND ----------

def read_table(name):
    return spark.table(f"{CATALOG}.{name}")


def write_table(df, name):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{name}"))


only = None if JOB_ID == "ALL" else {JOB_ID}
if only and JOB_ID not in {j.job_id for j in JOBS}:
    raise ValueError(f"Unknown job_id {JOB_ID}; use ALL or one of {[j.job_id for j in JOBS]}")
counts = run_pipeline(spark, read_table, write_table, only)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check against the legacy output
# MAGIC Each migrated table is compared with the CSV the legacy job produced (SQL Server for T-SQL,
# MAGIC SAS OnDemand for SAS). Golden values are read as text and cast to the migrated column types, so
# MAGIC `1000.005` from SAS and `1000.005` from Spark compare exactly. A table **matches** when the row
# MAGIC counts agree and no row is missing from, or extra in, either side.

# COMMAND ----------

from pyspark.sql import functions as F

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
        raise AssertionError(f"{len(bad)} table(s) differ from legacy: {[r[1] for r in bad]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Investigate a mismatch
# MAGIC Set `table` and `golden` below to see rows that differ (only needed if the check above fails).

# COMMAND ----------

table, golden = "silver.claims", "stg_claims"    # e.g. "gold.loss_ratio", "rpt_loss_ratio"
mig = spark.table(f"{CATALOG}.{table}")
types = dict(mig.dtypes)
g = spark.read.option("header", True).csv(f"{GOLDEN_PATH}/{golden}.csv")
g = g.select(*[F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c)).cast(types[c.lower()]).alias(c.lower())
               for c in g.columns if c.lower() in types])
m = mig.select(*g.columns)
display(g.exceptAll(m).withColumn("side", F.lit("legacy only"))
         .unionByName(m.exceptAll(g).withColumn("side", F.lit("migrated only"))).limit(50))
