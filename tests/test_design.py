import json

from factory.agents import design as d
from factory.contracts.models import ColumnLineage, DiscoveryResult, Layer
from factory.llm.base import LLM


class FakeLLM(LLM):
    provider, model = "fake", "fake"

    def __init__(self, reply):
        self.reply = reply

    def complete(self, system, prompt, temperature=0.0, json_mode=False):
        return json.dumps(self.reply)

    def _call(self, *a):
        return ""


def lin(table, col, srcs, tr):
    return ColumnLineage(target_table=table, target_column=col, source_columns=srcs, transformation=tr)


def j12():
    t = "rpt.open_reserves"
    return DiscoveryResult(
        job_id="J12_rpt_open_reserves", input_tables=["stg.claims", "stg.policies"], output_tables=[t],
        summary="open reserves by band",
        column_lineage=[
            lin(t, "product_code", ["stg.policies.product_code"], "p.product_code"),
            lin(t, "age_band", ["stg.claims.claim_age_days"], "CASE WHEN ... END"),
            lin(t, "open_claims", [], "COUNT(*)"),
            lin(t, "total_reserve", ["stg.claims.reserve_amount"], "SUM(c.reserve_amount)"),
        ])


def test_layer_rules():
    assert d.layer_and_name("rpt.open_reserves") == (Layer.GOLD, "gold.open_reserves")
    assert d.layer_and_name("legacy.rpt_loss_ratio") == (Layer.GOLD, "gold.loss_ratio")
    assert d.layer_and_name("stg.claims") == (Layer.SILVER, "silver.claims")
    assert d.layer_and_name("legacy.stg_claim_payments") == (Layer.SILVER, "silver.claim_payments")
    assert d.layer_and_name("legacy.claim_totals") == (Layer.SILVER, "silver.claim_totals")


def test_source_types_read_from_ddl():
    t = d.source_types()
    assert t["dbo.claims.reserve_amount"] == ("decimal(18,2)", True)
    assert t["dbo.premiums.amount"] == ("decimal(18,3)", True)
    assert t["dbo.policies.start_date"] == ("date", False)


def test_guardrails_drop_invented_column_bad_dtype_and_bad_key():
    llm = FakeLLM({"primary_key": ["not_a_column"],
                   "columns": [
                       {"name": "total_reserve", "dtype": "decimal(18,2)", "nullable": True, "rule": ">= 0"},
                       {"name": "open_claims", "dtype": "money", "nullable": False},
                       {"name": "invented", "dtype": "string", "nullable": True}]})
    res = d.design(j12(), llm)
    c = res.spec.contracts[0]
    assert res.spec.layer == Layer.GOLD and res.spec.target_tables == ["gold.open_reserves"]
    assert {x.name for x in c.columns} == {"product_code", "age_band", "open_claims", "total_reserve"}
    assert c.primary_key == ["product_code", "age_band"]          # GROUP BY fallback
    assert next(x for x in c.columns if x.name == "total_reserve").rule == ">= 0"
    assert next(x for x in c.columns if x.name == "open_claims").dtype == "bigint"   # bad dtype -> baseline
    notes = " ".join(res.notes)
    assert "invented" in notes and "money" in notes and "primary key" in notes


def test_valid_llm_key_used_and_key_columns_not_nullable():
    llm = FakeLLM({"primary_key": ["product_code", "age_band"], "columns": []})
    c = d.design(j12(), llm).spec.contracts[0]
    assert c.primary_key == ["product_code", "age_band"]
    assert all(not x.nullable for x in c.columns if x.name in c.primary_key)


def test_without_llm_uses_parser_baseline():
    class NoLLM(FakeLLM):
        @property
        def available(self):
            return False
    res = d.design(j12(), NoLLM({}))
    c = res.spec.contracts[0]
    # SQL Server SUM(decimal(18,2)) returns decimal(38,2)
    assert next(x for x in c.columns if x.name == "total_reserve").dtype == "decimal(38,2)"
    assert any("no LLM" in n for n in res.notes)


# ------------------------------------------------------------- fallbacks on the real 12 jobs

def _design_all(monkeypatch=None):
    from factory.agents import discovery
    from factory.llm.base import EchoLLM
    found = discovery.discover(EchoLLM())
    types, cols = d.source_types(), d.source_columns_in_order()
    out = {}
    for r in sorted(found.values(), key=lambda x: (x.wave, x.job_id)):
        res = d.design(r, EchoLLM(), types, cols)
        for c in res.spec.contracts:
            out[c.table] = c
    return out


def test_fallback_keys_are_real_identifiers():
    c = _design_all()
    assert c["silver.customers"].primary_key == ["customer_id"]
    assert c["silver.claims"].primary_key == ["claim_id"]
    assert c["silver.policy_earned_premium"].primary_key == ["policy_id", "policy_year"]
    assert c["gold.broker_commission"].primary_key == ["broker_id", "written_year"]


def test_nullable_source_columns_stay_nullable():
    c = _design_all()
    col = lambda t, n: next(x for x in c[t].columns if x.name == n)
    assert col("silver.claims", "reserve_amount").nullable
    assert col("silver.premiums", "paid_date").nullable
    assert not col("silver.policies", "end_date").nullable        # COALESCE(end_date, 9999-12-31)


def test_computed_column_types():
    c = _design_all()
    dt = lambda t, n: next(x for x in c[t].columns if x.name == n).dtype
    assert dt("silver.customers", "age_years") == "int"            # DATEDIFF(YEAR, ...)
    assert dt("silver.claims", "claim_age_days") == "int"
    assert dt("silver.policy_earned_premium", "days_in_force") == "int"
    assert dt("silver.policy_earned_premium", "earned_premium") == "decimal(38,2)"   # ROUND(..., 2)
    assert dt("gold.premium_collections", "arrears") == "decimal(38,3)"
    assert dt("gold.premium_collections", "collection_rate_pct") == "decimal(9,2)"   # CAST AS DECIMAL(9,2)
    assert dt("gold.broker_commission", "written_year") == "int"


def test_sas_without_lineage_uses_golden_header(tmp_path, monkeypatch):
    (tmp_path / "claim_totals.csv").write_text(
        "claim_id,loss_date,payment_count,total_paid,small_claim_flag\n"
        "CL1,2024-01-02,2,105.50,Y\nCL2,,1,,Y\n")
    monkeypatch.setattr(d, "GOLDEN", tmp_path)
    notes = []
    cols = d.golden_header_columns("legacy.claim_totals", notes)
    assert [(x.name, x.dtype) for x in cols] == [
        ("claim_id", "string"), ("loss_date", "date"), ("payment_count", "bigint"),
        ("total_paid", "decimal(38,2)"), ("small_claim_flag", "string")]
    assert "golden output header" in notes[0]
