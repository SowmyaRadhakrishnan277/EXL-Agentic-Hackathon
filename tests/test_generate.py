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
    assert not (tmp_path / "migrated" / job.job_id / "test_job.py").exists()   # tests come after a pass
    assert (tmp_path / "migrated" / "__init__.py").exists()
    assert out.attempt == 1 and out.translation_notes == ["handled trailing spaces"]


def test_prompt_contains_traps_risks_and_contract(tmp_path):
    job, disc, spec = fixtures()
    llm = ScriptedLLM([good()])
    g.generate(job, disc, spec, llm, out_root=tmp_path)
    system, prompt = llm.prompts[0]
    assert "TRAILING spaces" in prompt                                # the job compares a string literal
    assert "nodupkey" not in prompt and "half AWAY from zero" not in prompt   # traps it cannot hit are left out
    assert "test_py" not in prompt                                    # no test file on every attempt
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


def test_traps_follow_the_legacy_code():
    assert "nodupkey" in g.traps_for("proc sort data=x nodupkey; by id; run;")
    assert "half AWAY" in g.traps_for("SELECT ROUND(amount, 2) FROM t")
    assert "half AWAY" not in g.traps_for("SELECT id FROM t")
    assert "match the data contract" in g.traps_for("SELECT id FROM t")       # always sent


def test_unit_tests_written_once_after_pass(tmp_path):
    job, disc, spec = fixtures()
    g.generate(job, disc, spec, ScriptedLLM([good()]), out_root=tmp_path)
    llm = ScriptedLLM([{"test_py": "```python\n" + GOOD_TEST + "```"}])
    assert g.generate_tests(job, llm, out_root=tmp_path) is None
    assert (tmp_path / job.job_id / "test_job.py").read_text() == GOOD_TEST
    assert GOOD_JOB in llm.prompts[0][1]


def test_markdown_fence_is_stripped(tmp_path):
    job, disc, spec = fixtures()
    fenced = {"job_py": "```python\n" + GOOD_JOB + "```", "notes": []}
    g.generate(job, disc, spec, ScriptedLLM([fenced]), out_root=tmp_path)
    assert (tmp_path / job.job_id / "job.py").read_text() == GOOD_JOB


def test_repair_asks_for_a_diagnosis_and_shows_input_rows_and_history(tmp_path):
    job, disc, spec = fixtures()
    report = ReconciliationReport(
        job_id=job.job_id, attempt=2, passed=False, runtime_seconds=1.0, engine="local-pyspark",
        tables=[TableDiff(table="silver.claims", legacy_rows=10, migrated_rows=11, mismatched_keys=1,
                          sample_mismatches=[{"claim_id": "C1", "problem": "row only in migrated output"}],
                          input_rows=[{"input_table": "dbo.claims", "claim_id": "C1", "status": "rejected  "}])])
    llm = ScriptedLLM([{"diagnosis": "status has trailing spaces", "change": "rtrim before compare",
                        "job_py": GOOD_JOB, "notes": []}])
    out = g.generate(job, disc, spec, llm, attempt=3, prev_code="OLD_CODE", report=report, out_root=tmp_path,
                     history=["attempt 1: NameError: name 'upper' is not defined", "attempt 2: 1 rows differ"])
    prompt = llm.prompts[0][1]
    assert '"diagnosis"' in prompt and "INPUT rows behind those samples" in prompt
    assert '"status": "rejected  "' in prompt                         # the trailing spaces are visible
    assert "attempt 1: NameError" in prompt
    assert out.translation_notes[0] == "diagnosis: status has trailing spaces"


def test_first_attempt_is_not_asked_for_a_diagnosis(tmp_path):
    job, disc, spec = fixtures()
    llm = ScriptedLLM([good()])
    g.generate(job, disc, spec, llm, out_root=tmp_path)
    assert '"diagnosis"' not in llm.prompts[0][1]


def test_repeat_of_a_failed_attempt_is_rejected(tmp_path):
    job, disc, spec = fixtures()
    same_but_reformatted = "# retry\n" + GOOD_JOB.replace("return {", "return  {")
    other = GOOD_JOB.replace("tables['dbo.claims']", "tables['dbo.claims'].distinct()")
    llm = ScriptedLLM([{"job_py": same_but_reformatted}, {"job_py": other}])
    out = g.generate(job, disc, spec, llm, attempt=2, out_root=tmp_path, tried=[GOOD_JOB])
    assert "same code as an earlier attempt" in llm.prompts[1][1]
    assert (tmp_path / job.job_id / "job.py").read_text() == other
    assert any("validation failure on try 1" in n for n in out.translation_notes)


def test_distance_from_legacy():
    from factory.agents.migrate import distance
    def rep(**kw):
        return ReconciliationReport(job_id="J", attempt=1, passed=False, runtime_seconds=1.0, engine="x",
                                    golden_checked=True, tables=[TableDiff(table="t", **kw)])
    assert distance(rep(legacy_rows=100, migrated_rows=97, mismatched_keys=3)) == 6
    assert distance(rep(legacy_rows=0, migrated_rows=0, contract_violations=["runtime error"])) is None
