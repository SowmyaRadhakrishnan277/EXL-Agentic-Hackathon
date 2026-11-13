"""Each planted trap, pinned to the legacy behaviour on a few hand-made rows."""
from datetime import date
from decimal import Decimal as D

import pytest

from factory import catalog
from reference_jobs import jobs as J


def df(spark, schema, rows):
    return spark.createDataFrame(rows, schema)


def rows(out, *cols, order=None):
    return [tuple(r[c] for c in cols) for r in out.orderBy(*(order or cols)).collect()]


def test_registry_matches_factory_catalog():
    for legacy, lake in J.LEGACY_TO_LAKEHOUSE.items():
        assert catalog.lakehouse_name(legacy) == lake
    for job in J.JOBS:
        assert all(i in J.LEGACY_TO_LAKEHOUSE for i in job.inputs), job.job_id


def test_j01_age_counts_year_boundaries(spark):
    c = df(spark, "customer_id string, first_name string, last_name string, date_of_birth date, "
                  "county string, customer_since date, segment string",
           [("C1", "a", "b", date(1990, 12, 1), " dublin ", None, "RETAIL"),
            ("C2", "a", "b", None, "Cork", None, "RETAIL")])
    out = J.j01_stg_customers(spark, {"dbo.customers": c})["silver.customers"]
    assert rows(out, "customer_id", "county", "age_years") == [("C1", "DUBLIN", 35), ("C2", "CORK", None)]


def test_j02_open_end_date_and_status_cleanup(spark):
    p = df(spark, "policy_id string, customer_id string, product_code string, broker_id string, channel string, "
                  "start_date date, end_date date, status string, annual_premium decimal(18,2), "
                  "payment_frequency string, sum_insured decimal(18,2)",
           [("P1", "C1", "MOT", None, "WEB", date(2023, 1, 1), None, " Active", D("100.00"), "MONTHLY", None)])
    pr = df(spark, "product_code string, product_name string, line_of_business string, base_annual_premium decimal(18,2)",
            [("MOT", "Motor", "MOTOR", D("1"))])
    out = J.j02_stg_policies(spark, {"dbo.policies": p, "dbo.products": pr})["silver.policies"]
    assert rows(out, "end_date", "rolling_flag", "status") == [(date(9999, 12, 31), "Y", "ACTIVE")]


def test_j04_missing_amount_is_small_in_sas(spark):
    cp = df(spark, "payment_id string, claim_id string, payment_date date, payment_type string, amount decimal(18,3)",
            [("X1", "C1", None, "INDEMNITY", None), ("X2", "C1", None, "INDEMNITY", D("99.999")),
             ("X3", "C1", None, "INDEMNITY", D("100")), ("X4", "C1", None, "INDEMNITY", D("1000")),
             ("X4", "C1", None, "INDEMNITY", D("1000"))])
    out = J.j04_stg_claim_payments(spark, {"dbo.claim_payments": cp})["silver.claim_payments"]
    assert out.columns == ["payment_id", "claim_id", "payment_type", "payment_date", "amount", "amount_band"]
    assert rows(out, "payment_id", "amount_band") == [("X1", "SMALL"), ("X2", "SMALL"), ("X3", "MEDIUM"), ("X4", "LARGE")]


def test_j05_rejected_filter_follows_sql_server_collation(spark):
    statuses = ["REJECTED", "rejected", "REJECTED  ", " Rejected", None, "OPEN"]
    c = df(spark, "claim_id string, policy_id string, loss_date date, reported_date date, claim_type string, "
                  "status string, reserve_amount decimal(18,2)",
           [(f"K{i}", "P1", None, date(2025, 12, 1), "X", s, None) for i, s in enumerate(statuses)])
    out = J.j05_stg_claims(spark, {"dbo.claims": c})["silver.claims"]
    # kept: leading-space variant and OPEN; NULL status is excluded (NULL <> x is unknown)
    assert rows(out, "status", "claim_age_days") == [(" Rejected", 30), ("OPEN", 30)]


def test_j06_decimal_rounding_half_away_from_zero(spark):
    p = df(spark, "policy_id string, product_code string, start_date date, end_date date, annual_premium decimal(18,2)",
           [("P1", "MOT", date(2025, 1, 1), date(9999, 12, 31), D("365.00")),
            ("P2", "MOT", date(2025, 1, 1), date(2025, 1, 1), D("1.825"))])     # 1.83 * 1 / 365
    out = J.j06_stg_policy_earned_premium(spark, {"stg.policies": p})["silver.policy_earned_premium"]
    got = {(r.policy_id, r.policy_year): (r.days_in_force, r.earned_premium) for r in out.collect()}
    assert got[("P1", 2025)] == (365, D("365.00"))
    assert got[("P2", 2025)] == (1, D("0.01"))
    assert ("P1", 2024) not in got


def test_j07_missing_expense_makes_total_missing_and_flag_y(spark):
    pay = df(spark, "payment_id string, claim_id string, payment_type string, payment_date date, "
                    "amount decimal(18,3), amount_band string",
             [("X1", "C1", "INDEMNITY", None, D("1000.005"), "LARGE"),
              ("X2", "C2", "INDEMNITY", None, D("600"), "MEDIUM"), ("X3", "C2", "EXPENSE", None, D("0.005"), "SMALL")])
    claims = df(spark, "claim_id string, policy_id string, loss_date date, claim_type string",
                [("C1", "P1", None, "THEFT"), ("C2", "P1", None, "THEFT"), ("C3", "P1", None, "THEFT")])
    out = J.j07_stg_claim_totals(spark, {"legacy.stg_claim_payments": pay, "stg.claims": claims})["silver.claim_totals"]
    assert out.columns[-2:] == ["small_claim_flag", "total_paid"]
    assert rows(out, "claim_id", "payment_count", "total_paid", "small_claim_flag") == [
        ("C1", 1, None, "Y"),                 # no expense rows -> total missing -> 'Y'
        ("C2", 2, D("600.01"), "N"),          # 600.005 rounds half away from zero
        ("C3", 0, None, "Y")]                 # no payments at all


def test_j08_dedups_payments_and_matches_payment_type_like_sql_server(spark):
    cp = df(spark, "payment_id string, claim_id string, payment_date date, payment_type string, amount decimal(18,3)",
            [("X1", "C1", date(2024, 3, 5), "INDEMNITY", D("10.005")),
             ("X1", "C1", date(2024, 3, 5), "INDEMNITY", D("10.005")),           # exact repeat
             ("X2", "C1", date(2024, 3, 9), "indemnity ", D("1.000")),            # CI + trailing space
             ("X3", "C1", None, "INDEMNITY", D("5"))])                             # no date -> excluded
    claims = df(spark, "claim_id string, policy_id string", [("C1", "P1")])
    pol = df(spark, "policy_id string, product_code string", [("P1", "MOT")])
    out = J.j08_rpt_monthly_claims_paid(spark, {"dbo.claim_payments": cp, "stg.claims": claims,
                                                "stg.policies": pol})["gold.monthly_claims_paid"]
    assert rows(out, "payment_month", "claims_paid", "payments", "indemnity_paid") == [
        (date(2024, 3, 1), 1, 2, D("11.01"))]


def test_j09_counts_null_amount_instalments(spark):
    pm = df(spark, "due_date date, amount decimal(18,3), paid_flag string",
            [(date(2024, 1, 10), D("100"), "Y"), (date(2024, 1, 20), None, "Y"), (date(2024, 1, 25), D("50"), "N")])
    out = J.j09_rpt_premium_collections(spark, {"stg.premiums": pm})["gold.premium_collections"]
    r = out.first()
    assert (r.instalments_due, r.amount_due, r.amount_collected, r.arrears, r.collection_rate_pct) == \
           (3, D("150.000"), D("100.000"), D("50.000"), D("66.67"))


def test_j12_open_matches_case_and_trailing_but_not_leading_space(spark):
    claims = df(spark, "claim_id string, policy_id string, status string, claim_age_days int, reserve_amount decimal(18,2)",
                [("C1", "P1", "OPEN", 10, D("5")), ("C2", "P1", "open", 40, None),
                 ("C3", "P1", "OPEN  ", None, D("1")), ("C4", "P1", " Open", 10, D("9"))])
    pol = df(spark, "policy_id string, product_code string", [("P1", "MOT")])
    out = J.j12_rpt_open_reserves(spark, {"stg.claims": claims, "stg.policies": pol})["gold.open_reserves"]
    assert rows(out, "age_band", "open_claims", "claims_with_reserve", "total_reserve") == [
        ("0-30", 1, 1, D("5.00")), ("180+", 1, 1, D("1.00")), ("31-90", 1, 0, None)]


def test_j10_null_loss_date_is_its_own_group(spark):
    ct = df(spark, "claim_id string, policy_id string, loss_date date, indemnity_paid decimal(18,3)",
            [("C1", "P1", date(2024, 5, 1), D("50")), ("C2", "P1", None, D("7"))])
    pol = df(spark, "policy_id string, product_code string", [("P1", "MOT")])
    epp = df(spark, "policy_id string, product_code string, policy_year int, earned_premium decimal(18,2)",
             [("P1", "MOT", 2024, D("200.00")), ("P1", "MOT", 2025, D("0.00"))])
    out = J.j10_rpt_loss_ratio(spark, {"legacy.claim_totals": ct, "stg.policies": pol,
                                       "stg.policy_earned_premium": epp})["gold.loss_ratio"]
    assert rows(out, "year", "incurred", "claim_count", "loss_ratio", order=["year"]) == [
        (None, D("7.000"), 1, None), (2024, D("50.000"), 1, D("0.2500")), (2025, None, None, None)]
