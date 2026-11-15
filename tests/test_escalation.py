"""Local-first escalation and the OpenRouter budget. No Spark and no network: the lakehouse,
the reconciler and the HTTP call are replaced with stand-ins."""
import json

import pytest

from factory.agents import migrate, reconcile
from factory.contracts.models import (ColumnContract, DataContract, DesignSpec, DiscoveryResult, JobStatus,
                                      LegacyJob, Layer, ReconciliationReport, SourceLanguage, TableDiff)
from factory.llm import openrouter
from factory.llm.base import LLM, LLMBudgetError
from factory.runtime import lakehouse

CODE = """class DF:
    columns = ['claim_id']
    x = {x}
    def toDF(self, *names): return self
    def select(self, *names): return self

def run(spark, tables):
    return {{'silver.claims': DF()}}
"""


class FakeLLM(LLM):
    """Replies with CODE using the next x value; records every prompt."""
    def __init__(self, name, xs):
        self.provider, self.model, self.xs, self.prompts = name, "m", list(xs), []

    def complete(self, system, prompt, temperature=0.0, json_mode=False):
        self.prompts.append(prompt)
        if not self.xs:
            raise LLMBudgetError("daily budget used")
        return json.dumps({"job_py": CODE.format(x=self.xs.pop(0)), "notes": []})

    def _call(self, *a):
        return ""


@pytest.fixture
def job(monkeypatch, tmp_path):
    j = LegacyJob(job_id="J05_stg_claims", language=SourceLanguage.TSQL, path="x.sql",
                  code="SELECT * INTO stg.claims FROM dbo.claims WHERE status <> 'REJECTED'")
    disc = DiscoveryResult(job_id=j.job_id, input_tables=["dbo.claims"], output_tables=["stg.claims"], wave=1,
                           column_lineage=[], summary="claims staging")
    spec = DesignSpec(job_id=j.job_id, layer=Layer.SILVER, target_tables=["silver.claims"],
                      contracts=[DataContract(table="silver.claims", primary_key=["claim_id"],
                                              columns=[ColumnContract(name="claim_id", dtype="string")])])

    def check(spark, job_id, attempt, outputs, contracts, legacy_of, started):   # only x == 99 reconciles
        ok = outputs["silver.claims"].x == 99
        return ReconciliationReport(job_id=job_id, attempt=attempt, passed=ok, runtime_seconds=0.1, engine="fake",
                                    golden_checked=True,
                                    tables=[TableDiff(table="silver.claims", legacy_rows=10, migrated_rows=10,
                                                      mismatched_keys=0 if ok else 3)])
    monkeypatch.setattr(reconcile, "check", check)
    monkeypatch.setattr(reconcile, "add_evidence", lambda *a: None)
    monkeypatch.setattr(lakehouse, "load_inputs", lambda spark, tables: {})
    monkeypatch.setattr(lakehouse, "write_table", lambda df, table: None)
    return j, disc, spec, tmp_path / "migrated"


def run_job(job, llm, trail, **kw):
    j, disc, spec, out = job
    return migrate.migrate_job(None, j, disc, spec, llm, out, log=lambda *_: None, tests=False, trail=trail, **kw)


def test_second_model_carries_on_from_the_first(job):
    trail = migrate.Trail()
    local = FakeLLM("local", [1, 2, 3])
    rec, reports, _ = run_job(job, local, trail)
    assert rec.status == JobStatus.NEEDS_REVIEW and rec.attempts == 3

    hosted = FakeLLM("hosted", [99])
    rec, reports, _ = run_job(job, hosted, trail)
    assert rec.status == JobStatus.REPAIRED and rec.attempts == 4            # numbering continues
    assert [r.passed for r in reports] == [False, False, False, True]
    first = hosted.prompts[0]
    assert "ATTEMPT 4" in first and "x = 3" in first                         # sees the last local code
    assert "attempt 1: 3 rows differ" in first and "attempt 3: 3 rows differ" in first


def test_budget_running_out_stops_the_job_cleanly(job):
    trail = migrate.Trail()
    run_job(job, FakeLLM("local", [1, 2, 3]), trail)
    rec, reports, notes = run_job(job, FakeLLM("hosted", []), trail)         # no requests left
    assert rec.status == JobStatus.NEEDS_REVIEW and rec.attempts == 3 and len(reports) == 3
    assert any("daily budget used" in n for n in notes)


# ------------------------------------------------------------------ OpenRouter provider

@pytest.fixture
def router(monkeypatch, tmp_path):
    monkeypatch.setattr(openrouter, "USAGE", tmp_path / "usage.json")
    monkeypatch.setattr(openrouter, "CACHE", tmp_path)
    monkeypatch.setattr(openrouter.time, "sleep", lambda s: None)
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")
    monkeypatch.setenv("OPENROUTER_MODEL", "a:free,b:free")
    monkeypatch.setenv("OPENROUTER_DAILY_LIMIT", "3")

    def make(replies):
        llm = openrouter.OpenRouterLLM()
        sent = []

        def post(body):
            openrouter._count_request()
            sent.append(body)
            return replies.pop(0)
        llm._post = post
        return llm, sent
    return make


def ok(text):
    return {"choices": [{"message": {"content": text}}]}


def err(code, message=""):
    return {"error": {"code": code, "message": message}}


def test_counts_requests_and_reports_budget(router):
    llm, sent = router([ok("<think>hmm</think>{\"a\": 1}")])
    assert llm._call("s", "p", 0.0, True) == '{"a": 1}'                      # reasoning text removed
    assert openrouter.used_today() == 1 and llm.budget_left == 2
    assert sent[0]["response_format"] == {"type": "json_object"} and sent[0]["model"] == "a:free"


def test_daily_budget_is_enforced_before_sending(monkeypatch, router):
    llm, _ = router([])
    monkeypatch.setattr(openrouter, "used_today", lambda: 3)
    with pytest.raises(LLMBudgetError):
        openrouter.OpenRouterLLM._post(llm, {})


def test_provider_daily_limit_becomes_a_budget_error(router):
    llm, _ = router([err(429, "Rate limit exceeded: free-models-per-day")])
    with pytest.raises(LLMBudgetError):
        llm._call("s", "p", 0.0, False)


def test_unavailable_model_falls_back_to_the_next(router):
    llm, sent = router([err(404, "No endpoints found"), ok("hello")])
    assert llm._call("s", "p", 0.0, False) == "hello"
    assert [b["model"] for b in sent] == ["a:free", "b:free"] and llm.model == "b:free"


def test_model_without_json_mode_is_asked_in_plain_text(router):
    llm, sent = router([err(400, "response_format is not supported"), ok('{"a": 1}')])
    assert llm._call("s", "p", 0.0, True) == '{"a": 1}'
    assert "response_format" not in sent[1] and sent[1]["model"] == "a:free"


def test_json_with_prose_around_it_is_still_parsed():
    class Chatty(LLM):
        def _call(self, *a):
            return ""

        def complete(self, *a, **k):
            return 'Here is the result:\n{"job_py": "x = 1"}\nHope that helps.'
    assert Chatty().complete_json("s", "p") == {"job_py": "x = 1"}


# ------------------------------------------------------------------ lessons memory

def test_lessons_are_recorded_at_once_and_upgraded_when_fixed(tmp_path):
    from factory.agents.lessons import Lessons
    mem = Lessons(tmp_path / "lessons.json")
    mem.failure("J05_stg_claims", "tsql", "NameError: name 'upper' is not defined")
    mem.failure("J09_rpt_premium_collections", "tsql", "NameError: name 'count' is not defined")   # same mistake
    mem.failure("J04_stg_claim_payments", "sas", "107 rows differ from legacy output")             # not an error
    assert len(mem.items) == 1 and mem.items[0]["seen"] == 2 and mem.items[0]["jobs"] == ["J05", "J09"]
    assert "not yet fixed" in mem.for_prompt("J11_rpt_broker_commission", "tsql")

    mem.fixed("J05_stg_claims", "tsql", "NameError: name 'upper' is not defined",
              "diagnosis: upper was never imported change: use F.upper", "ollama/qwen")
    text = mem.for_prompt("J11_rpt_broker_commission", "tsql")
    assert "fixed" in text and "use F.upper" in text and "not yet fixed" not in text
    assert Lessons(tmp_path / "lessons.json").items[0]["confirmed"] is True                         # saved to disk


# ------------------------------------------------------------------ two lanes

class LaneLLM(LLM):
    """Per job: the x values to reply with (99 reconciles). Records which jobs it was asked about."""
    def __init__(self, name, plan, gate=None, release=None):
        self.provider, self.model, self.plan, self.asked = name, "m", {k: list(v) for k, v in plan.items()}, []
        self.gate, self.release, self.prompts = gate, release, {}

    def complete(self, system, prompt, temperature=0.0, json_mode=False):
        import re
        jid = re.search(r"job (J\d\d_\w+)", prompt).group(1)
        self.asked.append(jid[:3])
        self.prompts.setdefault(jid[:3], []).append(prompt)
        if self.gate is not None:
            assert self.gate.wait(10), "the local lane never ran while the hosted lane was busy"
        table = {"J05": "silver.claims", "J06": "silver.policy_earned_premium", "J12": "gold.open_reserves"}[jid[:3]]
        code = CODE.format(x=self.plan[jid[:3]].pop(0)).replace("'silver.claims'", repr(table))
        if self.release is not None and jid[:3] == "J06":
            self.release.set()
        return json.dumps({"diagnosis": "leading space matters", "change": "rtrim, not trim", "job_py": code})

    def _call(self, *a):
        return ""


def test_local_lane_keeps_working_while_a_job_is_on_the_hosted_lane(job, monkeypatch, tmp_path):
    import threading
    from factory.agents.design import DesignResult
    from factory.agents.lessons import Lessons
    run = tmp_path / "run"
    tables = {"J05_stg_claims": ("stg.claims", "silver.claims", []),
              "J06_stg_policy_earned_premium": ("stg.policy_earned_premium", "silver.policy_earned_premium", []),
              "J12_rpt_open_reserves": ("rpt.open_reserves", "gold.open_reserves", ["J05_stg_claims"])}
    for sub in ("discovery", "design"):
        (run / sub).mkdir(parents=True)
    for jid, (legacy, lake, deps) in tables.items():
        (run / "discovery" / f"{jid}.json").write_text(DiscoveryResult(
            job_id=jid, input_tables=["dbo.claims"], output_tables=[legacy], wave=2 if deps else 1,
            depends_on=deps, column_lineage=[], summary="s").model_dump_json())
        spec = DesignSpec(job_id=jid, layer=Layer.GOLD if lake.startswith("gold") else Layer.SILVER,
                          target_tables=[lake],
                          contracts=[DataContract(table=lake, primary_key=["claim_id"],
                                                  columns=[ColumnContract(name="claim_id", dtype="string")])])
        (run / "design" / f"{jid}.json").write_text(DesignResult(spec=spec, notes=[]).model_dump_json())

    def check(spark, job_id, attempt, outputs, contracts, legacy_of, started):
        ok = next(iter(outputs.values())).x == 99
        return ReconciliationReport(job_id=job_id, attempt=attempt, passed=ok, runtime_seconds=0.1, engine="fake",
                                    golden_checked=True,
                                    tables=[TableDiff(table=contracts[0].table, legacy_rows=10, migrated_rows=10,
                                                      mismatched_keys=0 if ok else 3)])
    monkeypatch.setattr(reconcile, "check", check)
    monkeypatch.setattr(lakehouse, "ensure_schemas", lambda spark: None)

    j06_done = threading.Event()
    local = LaneLLM("local", {"J05": [1, 2, 3], "J06": [99], "J12": [99]}, release=j06_done)
    hosted = LaneLLM("hosted", {"J05": [99]}, gate=j06_done)        # waits until the local lane has run J06
    mem = Lessons(tmp_path / "lessons.json")
    recs = migrate.migrate(run, local, spark=object(), out_root=tmp_path / "migrated", tests=False,
                           escalate=hosted, lessons=mem)
    by = {r.job_id[:3]: r for r in recs}
    assert by["J05"].status == JobStatus.REPAIRED and by["J05"].attempts == 4
    assert by["J06"].status == JobStatus.MATCHED and by["J12"].status == JobStatus.MATCHED
    assert hosted.asked == ["J05"]                                   # hosted quota only for the hard job
    assert local.asked.index("J06") < local.asked.index("J12")       # J12 waited for J05, J06 did not
    saved = json.loads((run / "migrate" / "J05_stg_claims.json").read_text())
    assert saved["llm"] == "hosted/m" and saved["escalated_from"] == "local/m"
    assert "rtrim, not trim" in local.prompts["J12"][0]              # J05's proven fix reached the next job
