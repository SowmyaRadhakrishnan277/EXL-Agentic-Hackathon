import json

import pytest
from openpyxl import load_workbook

from factory import mapping_excel
from factory.agents import design as d
from factory.agents import discovery
from factory.contracts.models import JobRecord, JobStatus, ReconciliationReport, TableDiff
from factory.llm.base import EchoLLM


@pytest.fixture(scope="module")
def run_dir(tmp_path_factory):
    run = tmp_path_factory.mktemp("run")
    (run / "discovery").mkdir()
    (run / "design").mkdir()
    found = discovery.discover(EchoLLM())
    types, cols = d.source_types(), d.source_columns_in_order()
    for r in sorted(found.values(), key=lambda x: (x.wave, x.job_id)):
        (run / "discovery" / f"{r.job_id}.json").write_text(r.model_dump_json())
        (run / "design" / f"{r.job_id}.json").write_text(d.design(r, EchoLLM(), types, cols).model_dump_json())
    return run


def _rows(ws):
    return list(ws.iter_rows(min_row=2, values_only=True))


def test_mapping_workbook_has_every_table_job_and_column(run_dir, tmp_path):
    wb = load_workbook(mapping_excel.write(run_dir, tmp_path / "m.xlsx"))
    assert wb.sheetnames == ["Overview", "Table Mapping", "Job Mapping", "Column Mapping"]
    tables = _rows(wb["Table Mapping"])
    assert len(tables) == 19                                    # 7 bronze + 12 job outputs
    assert {r[2] for r in tables} == {"bronze", "silver", "gold"}
    by_legacy = {r[0]: r for r in tables}
    assert by_legacy["stg.claims"][1:3] == ("silver.claims", "silver")
    assert by_legacy["rpt.open_reserves"][6] == "product_code, age_band"
    assert by_legacy["dbo.claims"][7] == "workspace.bronze.claims"
    assert len(_rows(wb["Job Mapping"])) == 12
    cols = _rows(wb["Column Mapping"])
    age = next(r for r in cols if r[3] == "silver.customers" and r[5] == "age_years")
    assert age[6] == "int" and age[9] == "bronze.customers.date_of_birth" and "DATEDIFF" in age[10]
    assert wb["Overview"]["E5"].value == "=COUNTIF('Table Mapping'!$C:$C,A5)"   # live counts


def test_status_sheet_appears_after_migration(run_dir, tmp_path):
    mig = run_dir / "migrate"
    mig.mkdir(exist_ok=True)
    rec = JobRecord(job_id="J05_stg_claims", language="tsql", wave=1, layer="silver",
                    status=JobStatus.REPAIRED, attempts=2, agent_seconds=12.5)
    rep = ReconciliationReport(job_id="J05_stg_claims", attempt=2, passed=True, runtime_seconds=2.0,
                               engine="local-pyspark", golden_checked=True,
                               tables=[TableDiff(table="silver.claims", legacy_rows=6415, migrated_rows=6415)])
    (mig / "J05_stg_claims.json").write_text(json.dumps(
        {"record": rec.model_dump(mode="json"), "reports": [rep.model_dump(mode="json")], "notes": []}))
    wb = load_workbook(mapping_excel.write(run_dir, tmp_path / "m.xlsx"))
    assert "Migration Status" in wb.sheetnames
    row = _rows(wb["Migration Status"])[0]
    assert row[1:2] + row[5:11] == ("J05_stg_claims", "repaired", 2, "Legacy output", 6415, 6415, 0)


def test_refresh_never_breaks_a_run(tmp_path, capsys):
    mapping_excel.refresh(tmp_path / "missing_run")
    assert "warning" in capsys.readouterr().out
