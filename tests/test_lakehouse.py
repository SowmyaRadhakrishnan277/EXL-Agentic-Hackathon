import subprocess
import sys

import pytest

from factory import catalog
from factory.contracts.models import Layer


def test_catalog_maps_every_legacy_table_to_one_layer():
    assert catalog.layer_and_name("dbo.claims") == (Layer.BRONZE, "bronze.claims")
    assert catalog.lakehouse_name("stg.claims") == "silver.claims"
    assert catalog.lakehouse_name("legacy.stg_claim_payments") == "silver.claim_payments"
    assert catalog.lakehouse_name("legacy.claim_totals") == "silver.claim_totals"
    assert catalog.lakehouse_name("rpt.open_reserves") == "gold.open_reserves"
    assert catalog.lakehouse_name("legacy.rpt_loss_ratio") == "gold.loss_ratio"
    assert catalog.lakehouse_column("stg.policies.policy_id") == "silver.policies.policy_id"


def test_uc_name_uses_catalog_env(monkeypatch):
    monkeypatch.setenv("UC_CATALOG", "exl")
    assert catalog.uc_name("gold.loss_ratio") == "exl.gold.loss_ratio"


def test_collisions_are_rejected():
    catalog.check_no_collisions(["stg.claims", "dbo.claims", "rpt.claims"])
    with pytest.raises(ValueError):
        catalog.check_no_collisions(["stg.claims", "legacy.stg_claims"])


def test_bronze_schema_follows_ddl():
    from factory.bronze import bronze_ddl
    assert bronze_ddl("premiums") == ("premium_id STRING, policy_id STRING, due_date DATE, "
                                      "paid_date DATE, amount DECIMAL(18,3)")


def test_bronze_ingest_keeps_raw_data_as_is(spark, tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    subprocess.run([sys.executable, "-m", "data.generator.generate", "--policies", "400",
                    "--messy", "0.1", "--out", str(raw)], check=True, capture_output=True)
    monkeypatch.setenv("LAKEHOUSE_DIR", str(tmp_path / "lake"))
    monkeypatch.delenv("SPARK_ENGINE", raising=False)
    from factory import bronze
    from factory.runtime import lakehouse
    counts = bronze.ingest(spark, str(raw))

    for t, n in counts.items():                       # every CSV row loaded, nothing dropped
        assert n == sum(1 for _ in open(raw / f"{t}.csv")) - 1
    df = lakehouse.read_table(spark, "bronze.premiums")
    assert [f.name for f in df.schema.fields][-2:] == ["_ingested_at", "_source_file"]
    assert str(df.schema["amount"].dataType) == "DecimalType(18,3)"
    assert df.filter("amount IS NULL").count() > 0                       # NULLs kept
    statuses = {r[0] for r in lakehouse.read_table(spark, "bronze.policies").select("status").collect()}
    assert any(s != s.strip() or s != s.upper() for s in statuses if s)   # messy text kept

    inputs = lakehouse.load_inputs(spark, ["dbo.claims", "dbo.premiums"])
    assert set(inputs) == {"dbo.claims", "dbo.premiums"}
    assert "_ingested_at" not in inputs["dbo.premiums"].columns           # jobs see legacy columns
