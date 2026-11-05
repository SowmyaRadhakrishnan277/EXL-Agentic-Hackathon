"""
Synthetic insurance data generator (Irish general insurer, 2021-2025).

    python -m data.generator.generate               # writes CSVs to data/raw/
    python -m data.generator.generate --policies 5000 --out /tmp/x

Deterministic: the same --seed always gives byte-identical CSVs, so every
teammate (SQL Server, SAS OnDemand, Databricks) works on the same data.

Messy data is injected on purpose (see docs/data_quirks.md) so the legacy
jobs can contain the kinds of edge cases that break naive migrations.
"""
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

START = np.datetime64("2021-01-01")
END = np.datetime64("2025-12-31")
DAYS = int((END - START).astype(int)) + 1

PRODUCTS = [
    # code, name, line of business, base annual premium, mean claim severity, annual claim freq
    ("MOT-STD", "Motor Standard", "MOTOR", 780.0, 2600.0, 0.11),
    ("MOT-PRM", "Motor Comprehensive Plus", "MOTOR", 1150.0, 3400.0, 0.12),
    ("HOM-BLD", "Home Buildings", "HOME", 420.0, 4100.0, 0.05),
    ("HOM-CNT", "Home Contents", "HOME", 190.0, 1300.0, 0.06),
    ("TRV-SGL", "Travel Single Trip", "TRAVEL", 45.0, 650.0, 0.04),
    ("TRV-ANN", "Travel Annual Multi-Trip", "TRAVEL", 110.0, 700.0, 0.07),
    ("PET-STD", "Pet Cover", "PET", 260.0, 480.0, 0.22),
    ("LND-STD", "Landlord Cover", "HOME", 540.0, 3800.0, 0.06),
]
COUNTIES = ["Dublin", "Cork", "Galway", "Limerick", "Kildare", "Meath", "Wicklow",
            "Wexford", "Waterford", "Kerry", "Louth", "Donegal", "Mayo", "Clare", "Tipperary"]
COUNTY_W = np.array([28, 12, 6, 5, 5, 4, 3, 3, 3, 3, 3, 3, 2, 2, 3], dtype=float)
FREQ_MONTHS = {"MONTHLY": 1, "QUARTERLY": 3, "ANNUAL": 12}


def _dates(rng, n, lo=START, hi=END):
    span = (hi - lo).astype(int) + 1
    return lo + rng.integers(0, span, n).astype("timedelta64[D]")


def _half_cent(rng, amounts: np.ndarray, rate: float) -> np.ndarray:
    """Turn a share of 2-dp amounts into exact x.xx5 values (rounding trap)."""
    out = np.round(amounts, 2)
    hit = rng.random(len(out)) < rate
    out[hit] = np.floor(out[hit] * 100) / 100 + 0.005
    return np.round(out, 3)


def _messy_text(rng, s: pd.Series, rate: float) -> pd.Series:
    """Add trailing spaces / change case on a share of values (string trap)."""
    s = s.copy()
    idx = s.index[rng.random(len(s)) < rate]
    kinds = rng.integers(0, 3, len(idx))
    s.loc[idx[kinds == 0]] = s.loc[idx[kinds == 0]] + "  "
    s.loc[idx[kinds == 1]] = s.loc[idx[kinds == 1]].str.lower()
    s.loc[idx[kinds == 2]] = " " + s.loc[idx[kinds == 2]].str.title()
    return s


def _nullify(rng, s: pd.Series, rate: float) -> pd.Series:
    s = s.copy().astype(object)
    s[rng.random(len(s)) < rate] = None
    return s


def _duplicate(rng, df: pd.DataFrame, rate: float) -> pd.DataFrame:
    """Append exact duplicates of a share of rows (dedup trap)."""
    dups = df.sample(frac=rate, random_state=int(rng.integers(1 << 31)))
    return pd.concat([df, dups]).sort_index(kind="stable").reset_index(drop=True)


def generate(n_policies: int, seed: int, messy: float) -> tuple[dict[str, pd.DataFrame], dict]:
    rng = np.random.default_rng(seed)
    fake = Faker("en_IE")
    Faker.seed(seed)
    quirks: dict[str, int] = {}

    # ---------------------------------------------------------------- products
    products = pd.DataFrame(PRODUCTS, columns=["product_code", "product_name", "line_of_business",
                                               "base_annual_premium", "_sev", "_freq"])

    # ---------------------------------------------------------------- brokers
    n_brokers = 40
    brokers = pd.DataFrame({
        "broker_id": [f"BR{i:03d}" for i in range(1, n_brokers + 1)],
        "broker_name": [fake.company() + " Insurance Brokers" for _ in range(n_brokers)],
        "county": rng.choice(COUNTIES, n_brokers, p=COUNTY_W / COUNTY_W.sum()),
        "commission_rate": np.round(rng.uniform(0.05, 0.15, n_brokers), 4),
        "active_flag": rng.choice(["Y", "N"], n_brokers, p=[0.9, 0.1]),
    })

    # ---------------------------------------------------------------- customers
    n_cust = int(n_policies * 0.7)
    dob = np.datetime64("1945-01-01") + rng.integers(0, 365 * 60, n_cust).astype("timedelta64[D]")
    customers = pd.DataFrame({
        "customer_id": [f"C{i:07d}" for i in range(1, n_cust + 1)],
        "first_name": [fake.first_name() for _ in range(n_cust)],
        "last_name": [fake.last_name() for _ in range(n_cust)],
        "date_of_birth": dob,
        "county": rng.choice(COUNTIES, n_cust, p=COUNTY_W / COUNTY_W.sum()),
        "customer_since": _dates(rng, n_cust, np.datetime64("2010-01-01"), END),
        "segment": rng.choice(["RETAIL", "SME"], n_cust, p=[0.92, 0.08]),
    })
    before = customers["county"].copy()
    customers["county"] = _messy_text(rng, customers["county"], messy)
    quirks["customers.county messy text"] = int((before != customers["county"]).sum())
    customers["date_of_birth"] = _nullify(rng, customers["date_of_birth"], messy / 2)
    quirks["customers.date_of_birth null"] = int(customers["date_of_birth"].isna().sum())

    # ---------------------------------------------------------------- policies
    pidx = rng.integers(0, len(products), n_policies)
    prod = products.iloc[pidx].reset_index(drop=True)
    start = _dates(rng, n_policies, START - np.timedelta64(365, "D"), END)
    single_trip = (prod["product_code"] == "TRV-SGL").to_numpy()
    term = np.where(single_trip, rng.integers(5, 30, n_policies), 365)
    renewals = rng.choice([0, 1, 2, 3, 4], n_policies, p=[0.35, 0.25, 0.18, 0.12, 0.10])
    term_total = term * (1 + np.where(single_trip, 0, renewals))
    end = start + term_total.astype("timedelta64[D]")
    status = np.where(end < END, "EXPIRED", "ACTIVE").astype(object)
    cancel = rng.random(n_policies) < 0.07
    cancel_date = start + (rng.random(n_policies) * term_total).astype("timedelta64[D]")
    end = np.where(cancel & (cancel_date < END), cancel_date, end)
    status[cancel & (cancel_date < END)] = "CANCELLED"
    lapse = (status == "EXPIRED") & (rng.random(n_policies) < 0.15)
    status[lapse] = "LAPSED"

    channel = rng.choice(["BROKER", "DIRECT", "ONLINE"], n_policies, p=[0.45, 0.25, 0.30])
    broker = np.where(channel == "BROKER", rng.choice(brokers["broker_id"], n_policies), None)
    loading = rng.lognormal(0, 0.25, n_policies)
    annual_premium = np.round(prod["base_annual_premium"].to_numpy() * loading, 2)
    freq = rng.choice(list(FREQ_MONTHS), n_policies, p=[0.45, 0.10, 0.45])
    freq = np.where(prod["line_of_business"] == "TRAVEL", "ANNUAL", freq)

    policies = pd.DataFrame({
        "policy_id": [f"P{i:08d}" for i in range(1, n_policies + 1)],
        "customer_id": rng.choice(customers["customer_id"], n_policies),
        "product_code": prod["product_code"],
        "broker_id": broker,
        "channel": channel,
        "start_date": start,
        "end_date": end,
        "status": status,
        "annual_premium": annual_premium,
        "payment_frequency": freq,
        "sum_insured": np.round(annual_premium * rng.uniform(20, 120, n_policies), -2),
    })
    # policies still ACTIVE beyond the data window keep their planned end date;
    # a share of open-ended (rolling) policies have NULL end_date
    rolling = (policies["status"] == "ACTIVE") & (rng.random(n_policies) < 0.2)
    policies.loc[rolling, "end_date"] = pd.NaT
    quirks["policies.end_date null (rolling cover)"] = int(rolling.sum())
    before = policies["status"].copy()
    policies["status"] = _messy_text(rng, policies["status"], messy)
    quirks["policies.status messy text"] = int((before != policies["status"]).sum())

    # ---------------------------------------------------------------- premiums
    p_start = policies["start_date"].to_numpy().astype("datetime64[D]")
    p_end = policies["end_date"].fillna(pd.Timestamp(END)).to_numpy().astype("datetime64[D]")
    p_end = np.minimum(p_end, END)
    step = policies["payment_frequency"].map(FREQ_MONTHS).to_numpy()
    n_months = np.maximum(((p_end - p_start).astype(int) // 30), 0)
    n_inst = np.maximum(n_months // step + 1, 1)
    rep = np.repeat(np.arange(n_policies), n_inst)
    k = np.concatenate([np.arange(c) for c in n_inst])
    due = p_start[rep].astype("datetime64[M]") + (k * step[rep]).astype("timedelta64[M]")
    due = due.astype("datetime64[D]") + (p_start[rep] - p_start[rep].astype("datetime64[M]").astype("datetime64[D]"))
    amt = policies["annual_premium"].to_numpy()[rep] * step[rep] / 12
    keep = (due >= START) & (due <= END)
    rep, due, amt = rep[keep], due[keep], amt[keep]
    unpaid = rng.random(len(rep)) < 0.03
    paid = due + rng.integers(-3, 20, len(rep)).astype("timedelta64[D]")
    premiums = pd.DataFrame({
        "premium_id": [f"PR{i:09d}" for i in range(1, len(rep) + 1)],
        "policy_id": policies["policy_id"].to_numpy()[rep],
        "due_date": due,
        "paid_date": pd.Series(paid).where(~unpaid, pd.NaT),
        "amount": _half_cent(rng, amt, messy),
    })
    quirks["premiums.amount half-cent"] = int((np.round(premiums["amount"] * 1000) % 10 == 5).sum())
    premiums["amount"] = _nullify(rng, premiums["amount"], messy / 2)
    quirks["premiums.amount null"] = int(premiums["amount"].isna().sum())
    quirks["premiums.paid_date null (unpaid)"] = int(premiums["paid_date"].isna().sum())

    # ---------------------------------------------------------------- claims
    exposure_years = (p_end - np.maximum(p_start, START)).astype(int).clip(min=0) / 365
    lam = prod["_freq"].to_numpy() * exposure_years
    n_claims = rng.poisson(lam)
    crep = np.repeat(np.arange(n_policies), n_claims)
    c_lo = np.maximum(p_start[crep], START)
    c_span = np.maximum((p_end[crep] - c_lo).astype(int), 1)
    loss = c_lo + (rng.random(len(crep)) * c_span).astype("timedelta64[D]")
    reported = np.minimum(loss + rng.geometric(0.15, len(crep)).astype("timedelta64[D]"), END)
    sev = prod["_sev"].to_numpy()[crep] * rng.lognormal(0, 0.9, len(crep))
    age_days = (END - reported).astype(int)
    c_status = np.where(age_days > 120, "CLOSED", "OPEN").astype(object)
    rejected = rng.random(len(crep)) < 0.08
    c_status[rejected] = "REJECTED"
    claim_types = {"MOTOR": ["COLLISION", "THEFT", "GLASS", "THIRD_PARTY"],
                   "HOME": ["WATER", "FIRE", "THEFT", "STORM"],
                   "TRAVEL": ["MEDICAL", "CANCELLATION", "BAGGAGE"],
                   "PET": ["ILLNESS", "INJURY"]}
    lob = prod["line_of_business"].to_numpy()[crep]
    ctype = np.array([rng.choice(claim_types[l]) for l in lob], dtype=object)
    claims = pd.DataFrame({
        "claim_id": [f"CL{i:08d}" for i in range(1, len(crep) + 1)],
        "policy_id": policies["policy_id"].to_numpy()[crep],
        "loss_date": loss,
        "reported_date": reported,
        "claim_type": ctype,
        "status": c_status,
        "reserve_amount": np.where(c_status == "OPEN", np.round(sev, 2), 0.0),
    })
    claims["loss_date"] = _nullify(rng, claims["loss_date"], messy / 2)
    quirks["claims.loss_date null"] = int(claims["loss_date"].isna().sum())
    claims["reserve_amount"] = _nullify(rng, claims["reserve_amount"], messy / 2)
    quirks["claims.reserve_amount null"] = int(claims["reserve_amount"].isna().sum())
    before = claims["status"].copy()
    claims["status"] = _messy_text(rng, claims["status"], messy)
    quirks["claims.status messy text"] = int((before != claims["status"]).sum())

    # ---------------------------------------------------------------- claim payments
    paid_mask = claims["status"].str.strip().str.upper() != "REJECTED"
    pay_claims = claims.index[paid_mask].to_numpy()
    n_pay = rng.integers(1, 4, len(pay_claims))
    prep = np.repeat(pay_claims, n_pay)
    part = rng.dirichlet(np.ones(3), len(prep))[:, 0] if len(prep) else np.array([])
    rep_dates = claims["reported_date"].to_numpy().astype("datetime64[D]")[prep]
    pay_date = np.minimum(rep_dates + rng.integers(5, 200, len(prep)).astype("timedelta64[D]"), END)
    sev_p = sev[prep] * np.clip(part * 1.5, 0.1, 1.0)
    claim_payments = pd.DataFrame({
        "payment_id": [f"CP{i:09d}" for i in range(1, len(prep) + 1)],
        "claim_id": claims["claim_id"].to_numpy()[prep],
        "payment_date": pay_date,
        "payment_type": rng.choice(["INDEMNITY", "EXPENSE"], len(prep), p=[0.85, 0.15]),
        "amount": _half_cent(rng, sev_p, messy),
    })
    quirks["claim_payments.amount half-cent"] = int((np.round(claim_payments["amount"] * 1000) % 10 == 5).sum())
    claim_payments["amount"] = _nullify(rng, claim_payments["amount"], messy / 2)
    quirks["claim_payments.amount null"] = int(claim_payments["amount"].isna().sum())
    n0 = len(claim_payments)
    claim_payments = _duplicate(rng, claim_payments, messy / 2)
    quirks["claim_payments duplicate rows"] = len(claim_payments) - n0
    n0 = len(premiums)
    premiums = _duplicate(rng, premiums, messy / 4)
    quirks["premiums duplicate rows"] = len(premiums) - n0

    tables = {
        "products": products.drop(columns=["_sev", "_freq"]),
        "brokers": brokers,
        "customers": customers,
        "policies": policies,
        "premiums": premiums,
        "claims": claims,
        "claim_payments": claim_payments,
    }
    return tables, quirks


def write_csvs(tables: dict[str, pd.DataFrame], out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        df = df.copy()
        for c in df.columns:
            if "date" in c or c == "customer_since":
                df[c] = pd.to_datetime(df[c]).dt.strftime("%Y-%m-%d")
        # empty field = NULL in SQL Server BULK INSERT, SAS and Spark
        df.to_csv(out / f"{name}.csv", index=False, na_rep="", float_format="%.3f")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--policies", type=int, default=50_000)
    ap.add_argument("--seed", type=int, default=2026)
    ap.add_argument("--messy", type=float, default=0.02, help="share of values made messy")
    ap.add_argument("--out", type=Path, default=Path("data/raw"))
    a = ap.parse_args()
    tables, quirks = generate(a.policies, a.seed, a.messy)
    write_csvs(tables, a.out)
    summary = {"seed": a.seed, "policies": a.policies, "messy_rate": a.messy,
               "generated_on": str(date.today()),
               "rows": {k: len(v) for k, v in tables.items()}, "quirks": quirks}
    (a.out / "_manifest.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
