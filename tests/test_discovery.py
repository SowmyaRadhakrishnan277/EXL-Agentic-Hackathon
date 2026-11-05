import json

from factory.agents import discovery as d
from factory.llm.base import LLM


class FakeLLM(LLM):
    """Stands in for Gemini: returns a fixed reply, including one invented column."""
    provider, model = "fake", "fake"

    def complete(self, system, prompt, temperature=0.0, json_mode=False):
        return self._call(system, prompt, temperature, json_mode)

    def _call(self, system, prompt, temperature, json_mode):
        if "SAS program J04" in prompt:
            return json.dumps({
                "columns": [
                    {"target_table": "legacy.stg_claim_payments", "target_column": "amount",
                     "source_columns": ["dbo.claim_payments.amount"], "transformation": "copy"},
                    {"target_table": "legacy.stg_claim_payments", "target_column": "amount_band",
                     "source_columns": ["dbo.claim_payments.amount", "dbo.claim_payments.currency"],
                     "transformation": "band"},
                ],
                "output_columns": {"legacy.stg_claim_payments": ["amount", "amount_band"]},
            })
        if "SAS program" in prompt:
            return json.dumps({"columns": [], "output_columns": {}})
        return json.dumps({"summary": "test summary", "risk_notes": ["note"]})


def test_tables_dependencies_and_waves_from_code():
    res = d.discover(FakeLLM())
    assert res["J10_rpt_loss_ratio"].wave == 3
    assert res["J07_stg_claim_totals"].depends_on == ["J04_stg_claim_payments", "J05_stg_claims"]
    assert res["J04_stg_claim_payments"].input_tables == ["dbo.claim_payments"]


def test_guardrail_drops_invented_columns():
    res = d.discover(FakeLLM())
    j04 = res["J04_stg_claim_payments"]
    band = next(c for c in j04.column_lineage if c.target_column == "amount_band")
    assert band.source_columns == ["dbo.claim_payments.amount"]          # 'currency' removed
    assert any("guardrail" in n and "currency" in n for n in j04.risk_notes)
    assert j04.summary == "test summary"


def test_csv_naming_rule():
    assert d.csv_to_table("stg_claims") == "stg.claims"
    assert d.csv_to_table("claim_payments") == "dbo.claim_payments"
