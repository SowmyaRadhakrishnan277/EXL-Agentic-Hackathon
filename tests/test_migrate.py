"""End-to-end test of the migration agent on real Spark, with a scripted stand-in for Gemini."""
import json
import subprocess
import sys

import pandas as pd
import pytest

from factory.agents import design as d
from factory.agents import discovery, migrate, reconcile
from factory.contracts.models import JobStatus
from factory.llm.base import LLM, EchoLLM

HEAD = """from pyspark.sql import functions as F

def run(spark, tables):
    c = tables["dbo.claims"]
"""
TAIL = """    out = c.select("claim_id", "policy_id", "loss_date", "reported_date", "claim_type", "status",
                   "reserve_amount",
                   F.datediff(F.lit("2025-12-31").cast("date"), F.col("reported_date")).alias("claim_age_days"))
    return {"silver.claims": out}
"""
CRASHES = HEAD + '    c = c.filter(F.col("claim_status") != "REJECTED")\n' + TAIL           # no such column
NAIVE = HEAD + '    c = c.filter(F.col("status") != "REJECTED")\n' + TAIL                  # exact compare
FIXED = HEAD + '    c = c.filter(F.upper(F.rtrim(F.col("status"))) != "REJECTED")\n' + TAIL  # SQL Server semantics
TEST = "def test_placeholder():\n    assert True\n"


class ScriptedLLM(LLM):
    provider, model = "scripted", "stand-in"

    def __init__(self, codes):
        self.codes, self.prompts = list(codes), []

    def complete(self, system, prompt, temperature=0.0, json_mode=False):
        self.prompts.append(prompt)
        code = self.codes.pop(0) if self.codes else self.codes_last
        self.codes_last = code
        return json.dumps({"job_py": code, "test_py": TEST, "notes": []})

    def _call(self, *a):
        return ""


@pytest.fixture(scope="module")
def setup(spark, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("mig")
    raw = tmp / "raw"
    subprocess.run([sys.executable, "-m", "data.generator.generate", "--policies", "5000",
                    "--messy", "0.15", "--out", str(raw)], check=True, capture_output=True)
    mp = pytest.MonkeyPatch()
    mp.setenv("LAKEHOUSE_DIR", str(tmp / "lake"))
    mp.delenv("SPARK_ENGINE", raising=False)
    from factory import bronze
    bronze.ingest(spark, str(raw), ["claims"])

    # run dir with real discovery + design output (parser only)
    run = tmp / "run"
    (run / "discovery").mkdir(parents=True)
    (run / "design").mkdir()
    found = discovery.discover(EchoLLM())
    types, cols = d.source_types(), d.source_columns_in_order()
    for r in sorted(found.values(), key=lambda x: (x.wave, x.job_id)):
        (run / "discovery" / f"{r.job_id}.json").write_text(r.model_dump_json())
        (run / "design" / f"{r.job_id}.json").write_text(d.design(r, EchoLLM(), types, cols).model_dump_json())

    # legacy output for J05 computed with SQL Server semantics:
    # '<>' ignores trailing spaces and case, but not leading spaces
    claims = pd.read_csv(raw / "claims.csv", dtype=str, keep_default_na=False)
    keep = claims[claims["status"].str.rstrip().str.upper() != "REJECTED"].copy()
    keep["claim_age_days"] = (pd.Timestamp("2025-12-31") - pd.to_datetime(keep["reported_date"])).dt.days
    golden = tmp / "golden"
    golden.mkdir()
    keep.to_csv(golden / "stg_claims.csv", index=False)
    mp.setattr(reconcile, "GOLDEN", golden)
    naive_rows = (claims["status"] != "REJECTED").sum()
    yield {"run": run, "tmp": tmp, "legacy_rows": len(keep), "naive_rows": naive_rows}
    mp.undo()


def test_trap_is_present_in_this_data(setup):
    assert setup["naive_rows"] > setup["legacy_rows"]      # otherwise the test proves nothing


def test_agent_fixes_crash_then_trap_and_writes_silver(spark, setup):
    llm = ScriptedLLM([CRASHES, NAIVE, FIXED])
    recs = migrate.migrate(setup["run"], llm, spark, only={"J05"}, out_root=setup["tmp"] / "migrated")
    rec = recs[0]
    assert rec.status == JobStatus.REPAIRED and rec.attempts == 3

    # attempt 2 was told about the crash, attempt 3 about the row differences
    assert "claim_status" in llm.prompts[1] and "runtime error" in llm.prompts[1]
    assert "rows differ by key" in llm.prompts[2] and "row only in migrated output" in llm.prompts[2]

    from factory.runtime import lakehouse
    assert lakehouse.read_table(spark, "silver.claims").count() == setup["legacy_rows"]
    saved = json.loads((setup["run"] / "migrate" / "J05_stg_claims.json").read_text())
    assert [r["passed"] for r in saved["reports"]] == [False, False, True]
    assert saved["reports"][-1]["golden_checked"] is True
    assert (setup["tmp"] / "migrated" / "J05_stg_claims" / "attempts" / "job_attempt2.py").exists()


def test_failed_job_blocks_downstream(spark, setup):
    llm = ScriptedLLM([CRASHES])                           # keeps returning broken code
    recs = migrate.migrate(setup["run"], llm, spark, only={"J05", "J12"},
                           out_root=setup["tmp"] / "migrated_b")
    by = {r.job_id[:3]: r for r in recs}
    assert by["J05"].status == JobStatus.NEEDS_REVIEW and by["J05"].attempts == 3
    assert by["J12"].status == JobStatus.BLOCKED


def test_safety_check_rejects_escape_hatches():
    assert migrate.safety_check("import os\ndef run(spark, tables):\n    return {}") is not None
    assert migrate.safety_check("def run(spark, tables):\n    open('x')\n    return {}") is not None
    assert migrate.safety_check(FIXED) is None


def test_contract_violations_are_specific(spark):
    from factory.contracts.models import ColumnContract, DataContract
    df = spark.createDataFrame([("A", 1), ("A", None)], "k string, v int")
    c = DataContract(table="silver.t", primary_key=["k"],
                     columns=[ColumnContract(name="k", dtype="string", nullable=False),
                              ColumnContract(name="v", dtype="decimal(18,2)", nullable=False)])
    v = " | ".join(reconcile.contract_violations(df, c))
    assert "v: type int" in v and "1 NULL values" in v and "not unique" in v
