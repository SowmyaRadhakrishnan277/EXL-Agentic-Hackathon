import json

import pytest

from factory.agents import generate as g
from factory.contracts.models import (ColumnContract, ColumnLineage, DataContract, DesignSpec,
                                      DiscoveryResult, LegacyJob, Layer, ReconciliationReport,
                                      SourceLanguage, TableDiff)
from factory.llm.base import LLM

GOOD_JOB = "def run(spark, tables):\n    return {'silver.claims': tables['dbo.claims']}\n"
GOOD_TEST = "def test_x():\n    assert True\n"


class ScriptedLLM(LLM):
    """Returns the queued replies in order and records every prompt it saw."""
    provider, model = "fake", "fake"

    def __init__(self, replies):
        self.replies, self.prompts = list(replies), []

    def complete(self, system, prompt, temperature=0.0, json_mode=False):
        self.prompts.append((system, prompt))
        return json.dumps(self.replies.pop(0))

    def _call(self, *a):
        return ""


def good(notes=("handled trailing spaces",)):
    return {"job_py": GOOD_JOB, "test_py": GOOD_TEST, "notes": list(notes)}


def fixtures():
    job = LegacyJob(job_id="J05_stg_claims", language=SourceLanguage.TSQL, path="x.sql",
                    code="SELECT * INTO stg.claims FROM dbo.claims WHERE status <> 'REJECTED'")
    disc = DiscoveryResult(
        job_id=job.job_id, input_tables=["dbo.claims"], output_tables=["stg.claims"], summary="claims staging",
        risk_notes=["status compare ignores trailing spaces"],
        column_lineage=[ColumnLineage(target_table="stg.claims", target_column="claim_id",
                                      source_columns=["dbo.claims.claim_id"], transformation="c.claim_id")])
    spec = DesignSpec(job_id=job.job_id, layer=Layer.SILVER, target_tables=["silver.claims"],
                      contracts=[DataContract(table="silver.claims", primary_key=["claim_id"],
                                              columns=[ColumnContract(name="claim_id", dtype="string",
                                                                      nullable=False)])])
    return job, disc, spec


def test_writes_files_and_record(tmp_path):
    job, disc, spec = fixtures()
    llm = ScriptedLLM([good()])
    out = g.generate(job, disc, spec, llm, out_root=tmp_path / "migrated")
    assert (tmp_path / "migrated" / job.job_id / "job.py").read_text() == GOOD_JOB
    assert (tmp_path / "migrated" / job.job_id / "test_job.py").exists()
    assert (tmp_path / "migrated" / "__init__.py").exists()
    assert out.attempt == 1 and out.translation_notes == ["handled trailing spaces"]


def test_prompt_contains_traps_risks_and_contract(tmp_path):
    job, disc, spec = fixtures()
    llm = ScriptedLLM([good()])
    g.generate(job, disc, spec, llm, out_root=tmp_path)
    system, prompt = llm.prompts[0]
    assert "TRAILING spaces" in system and "nodupkey" in system and "half AWAY from zero" in system
    assert "status compare ignores trailing spaces" in prompt        # discovery risk note
    assert "silver.claims" in prompt and "claim_id: string NOT NULL" in prompt
    assert "ATTEMPT" not in prompt                                    # not a repair run


def test_syntax_error_is_fed_back_and_retried(tmp_path):
    job, disc, spec = fixtures()
    llm = ScriptedLLM([{"job_py": "def run(spark, tables:\n", "test_py": GOOD_TEST}, good()])
    out = g.generate(job, disc, spec, llm, out_root=tmp_path)
    assert "syntax error" in llm.prompts[1][1]
    assert any("validation failure on try 1" in n for n in out.translation_notes)
    assert (tmp_path / job.job_id / "job.py").read_text() == GOOD_JOB


def test_missing_run_function_rejected_after_retries(tmp_path):
    job, disc, spec = fixtures()
    bad = {"job_py": "x = 1\n", "test_py": GOOD_TEST}
    llm = ScriptedLLM([bad] * (g.MAX_FIX + 1))
    with pytest.raises(ValueError, match="no valid translation"):
        g.generate(job, disc, spec, llm, out_root=tmp_path)
    assert not (tmp_path / job.job_id / "job.py").exists()           # nothing broken is written


def test_repair_prompt_carries_previous_code_and_diff(tmp_path):
    job, disc, spec = fixtures()
    report = ReconciliationReport(
        job_id=job.job_id, attempt=1, passed=False, runtime_seconds=1.0, engine="local-pyspark",
        tables=[TableDiff(table="silver.claims", legacy_rows=4120, migrated_rows=4390,
                          column_total_diffs={"reserve_amount": -1250.5}, mismatched_keys=270,
                          sample_mismatches=[{"claim_id": "C1", "legacy": None, "migrated": "rejected "}])])
    llm = ScriptedLLM([good()])
    out = g.generate(job, disc, spec, llm, attempt=2, prev_code="OLD_CODE", report=report, out_root=tmp_path)
    prompt = llm.prompts[0][1]
    assert "ATTEMPT 2" in prompt and "OLD_CODE" in prompt
    assert "legacy rows 4120, migrated rows 4390" in prompt and "-1250.5" in prompt
    assert out.attempt == 2
