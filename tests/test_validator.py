"""Two-system validator: comparison rules, sources and the command line. No Spark, no network."""
import json
import sqlite3
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import pytest
from openpyxl import load_workbook

from validator import ColumnRule, ConfigError, Rules, compare
from validator.cli import main
from validator.compare import NULL, canon


def df(rows, cols):
    return pd.DataFrame(rows, columns=cols)


LEGACY = df([["P1", "HOME", "100.50", "2024-01-31"], ["P2", "MOTOR", "20.00", "2024-02-01"],
             ["P3", "HOME", "", "2024-03-05"]], ["policy_id", "product", "premium", "start_date"])


def test_same_data_in_different_types_and_row_order_passes():
    cloud = df([["P3", "HOME", None, date(2024, 3, 5)], ["P1", "HOME", Decimal("100.5"), datetime(2024, 1, 31)],
                ["P2", "MOTOR", 20.0, pd.Timestamp("2024-02-01")]], ["POLICY_ID", "Product", "premium", "start_date"])
    r = compare("policies", LEGACY, cloud, keys=["policy_id"])
    assert r["status"] == "PASS" and r["matched_rows"] == 3 and r["cells_differing"] == 0
    totals = {c["column"]: (c["legacy_total"], c["cloud_total"]) for c in r["columns"]}
    assert totals["premium"] == ("120.5", "120.5")


def test_keys_101_and_101_point_0_and_text_101_are_one_key_but_leading_zeros_are_kept():
    legacy = df([["101", "a"], ["00123", "b"]], ["id", "v"])
    cloud = df([[101.0, "a"], ["123", "b"]], ["id", "v"])
    r = compare("ids", legacy, cloud, keys=["id"])
    assert r["matched_rows"] == 1                                    # 101 pairs with 101.0
    assert r["legacy_only_rows"] == 1 and r["cloud_only_rows"] == 1  # '00123' is not 123
    assert r["legacy_only"][0]["key"] == "id=00123"


def test_value_differences_are_listed_with_key_column_and_difference():
    cloud = LEGACY.copy()
    cloud.loc[1, "premium"] = "20.07"
    cloud.loc[0, "product"] = "home"
    r = compare("policies", LEGACY, cloud, keys=["policy_id"])
    assert r["status"] == "FAIL" and r["cells_differing"] == 2 and r["rows_differing"] == 2
    by_col = {m["column"]: m for m in r["mismatches"]}
    assert by_col["premium"] == {"check": "policies", "key": "policy_id=P2", "column": "premium",
                                 "legacy_value": "20", "cloud_value": "20.07", "difference": "-0.07"}
    assert by_col["product"]["legacy_value"] == "HOME" and by_col["product"]["cloud_value"] == "home"


def test_tolerance_rounding_and_case_rules_per_column():
    cloud = LEGACY.copy()
    cloud.loc[1, "premium"] = "20.004"
    cloud.loc[0, "product"] = "home "
    assert compare("p", LEGACY, cloud, keys=["policy_id"])["cells_differing"] == 2
    rules = Rules(columns={"premium": ColumnRule(abs=0.005), "product": ColumnRule(case_sensitive=False)})
    assert compare("p", LEGACY, cloud, keys=["policy_id"], rules=rules)["status"] == "PASS"
    rules = Rules(columns={"premium": ColumnRule(round=2), "product": ColumnRule(case_sensitive=False)})
    assert compare("p", LEGACY, cloud, keys=["policy_id"], rules=rules)["status"] == "PASS"


def test_null_markers_and_null_keys():
    assert canon("", ColumnRule()) is NULL and canon(" . ", ColumnRule()) is NULL and canon("NULL", ColumnRule()) is NULL
    assert canon(float("nan"), ColumnRule()) is NULL and canon(pd.NaT, ColumnRule()) is NULL
    legacy = df([["HOM", "", "5"], ["HOM", "2024", "7"]], ["product", "year", "n"])
    cloud = df([["HOM", None, 5], ["HOM", 2024, 7]], ["product", "year", "n"])
    assert compare("lr", legacy, cloud, keys=["product", "year"])["status"] == "PASS"
    cloud.loc[0, "n"] = None                                          # a value on one side, NULL on the other
    r = compare("lr", legacy, cloud, keys=["product", "year"])
    assert r["cells_differing"] == 1 and r["mismatches"][0]["cloud_value"] == "NULL"


def test_rows_on_one_side_only_schema_differences_and_duplicate_keys():
    cloud = pd.concat([LEGACY.iloc[[0, 1]], df([["P9", "PET", "1", "2024-01-01"]], LEGACY.columns)], ignore_index=True)
    cloud["loaded_at"] = "x"
    r = compare("p", LEGACY, cloud, keys=["policy_id"])
    assert r["status"] == "FAIL" and r["legacy_only_rows"] == 1 and r["cloud_only_rows"] == 1
    assert r["columns_only_in_cloud"] == ["loaded_at"] and "P3" in r["legacy_only"][0]["row"]
    assert compare("p", LEGACY, cloud, keys=["policy_id"], ignore=["loaded_at"])["columns_only_in_cloud"] == []
    twice = pd.concat([LEGACY, LEGACY.iloc[[0]]], ignore_index=True)
    r = compare("p", twice, LEGACY, keys=["policy_id"])
    assert r["duplicate_keys_legacy"] == 1 and r["legacy_only_rows"] == 1 and r["status"] == "FAIL"


def test_without_keys_whole_rows_are_compared_as_a_multiset():
    shuffled = LEGACY.iloc[[2, 0, 1]].reset_index(drop=True)
    assert compare("p", LEGACY, shuffled)["status"] == "PASS"
    changed = shuffled.copy()
    changed.loc[0, "product"] = "PET"
    r = compare("p", LEGACY, changed)
    assert r["keyless"] and r["legacy_only_rows"] == 1 and r["cloud_only_rows"] == 1


def test_column_map_and_bad_configuration():
    cloud = LEGACY.rename(columns={"premium": "annual_premium"})
    assert compare("p", LEGACY, cloud, keys=["policy_id"], column_map={"Premium": "annual_premium"})["status"] == "PASS"
    with pytest.raises(ConfigError, match="key column"):
        compare("p", LEGACY, cloud, keys=["nope"])
    with pytest.raises(ConfigError, match="no column in common"):
        compare("p", LEGACY, df([[1]], ["x"]))


def test_excel_against_excel_from_the_command_line(tmp_path, capsys):
    a, b = tmp_path / "legacy.xlsx", tmp_path / "cloud.xlsx"
    typed = df([["P1", "HOME", 100.5, datetime(2024, 1, 31)], ["P2", "MOTOR", 20, datetime(2024, 2, 1)],
                ["P3", "HOME", None, datetime(2024, 3, 5)]], LEGACY.columns)
    typed.to_excel(a, index=False, sheet_name="Extract")
    typed.to_excel(b, index=False)
    out = tmp_path / "ok"
    assert main(["--legacy", str(a), "--legacy-sheet", "Extract", "--cloud", str(b), "--keys", "policy_id", "--out", str(out)]) == 0
    typed.loc[1, "premium"] = 25
    typed.iloc[[0, 1]].to_excel(b, index=False)
    out = tmp_path / "bad"
    assert main(["--legacy", str(a), "--cloud", str(b), "--keys", "policy_id", "--out", str(out)]) == 1
    wb = load_workbook(out / "validation_report.xlsx")
    assert wb.sheetnames == ["Summary", "Columns", "Mismatches", "Legacy_Only", "Cloud_Only"]
    summary = dict(zip([c.value for c in wb["Summary"][1]], [c.value for c in wb["Summary"][2]]))
    assert summary["Status"] == "FAIL" and summary["Legacy only"] == 1 and summary["Cells differing"] == 1
    assert [c.value for c in wb["Mismatches"][2]][1:5] == ["policy_id=P2", "premium", "20", "25"]
    assert "policy_id=P3" in wb["Legacy_Only"][2][1].value
    result = json.loads((out / "validation_result.json").read_text())
    assert result["status"] == "FAIL" and result["failed"] == 1 and result["results"][0]["mismatches_sample"]
    assert "1 rows only in legacy" in capsys.readouterr().out


def test_config_file_with_a_database_connection_and_secrets_from_the_environment(tmp_path, monkeypatch, capsys):
    db = tmp_path / "legacy.db"
    with sqlite3.connect(db) as con:
        con.execute("create table rpt_totals (product text, total real)")
        con.executemany("insert into rpt_totals values (?, ?)", [("HOME", 10.25), ("MOTOR", 7.0)])
    pd.DataFrame({"product": ["HOME", "MOTOR"], "total": ["10.25", "7.00"]}).to_csv(tmp_path / "cloud.csv", index=False)
    cfg = tmp_path / "checks.yml"
    cfg.write_text(f"""
title: nightly totals
connections:
  legacy_db: {{type: sqlite, path: "{db}"}}
  prod_sql: {{type: sqlserver, host: db.internal, database: ins, user_env: VAL_TEST_USER, password_env: VAL_TEST_PW}}
checks:
  - name: totals
    legacy: {{connection: legacy_db, query: "select product, total from rpt_totals"}}
    cloud: {{file: cloud.csv}}
    keys: [product]
  - name: needs_secret
    legacy: {{connection: prod_sql, table: rpt.totals}}
    cloud: {{file: cloud.csv}}
""")
    monkeypatch.delenv("VAL_TEST_USER", raising=False)
    assert main([str(cfg), "--only", "totals", "--out", str(tmp_path / "o1")]) == 0
    assert main([str(cfg), "--out", str(tmp_path / "o2")]) == 2          # a check that cannot run -> exit code 2
    result = json.loads((tmp_path / "o2" / "validation_result.json").read_text())
    by = {r["check"]: r for r in result["results"]}
    assert by["totals"]["status"] == "PASS" and by["needs_secret"]["status"] == "ERROR"
    assert "VAL_TEST_USER is not set" in by["needs_secret"]["problems"][0]
    assert by["needs_secret"]["legacy_source"] == "sqlserver://db.internal/ins  rpt.totals"
    assert main([str(tmp_path / "missing.yml")]) == 2
    assert "not found" in capsys.readouterr().err
