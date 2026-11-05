"""
Run the legacy T-SQL jobs in dependency order on SQL Server and save golden outputs.

    python -m data.load_mssql            # once, loads the raw tables
    python -m legacy.run_legacy_tsql     # runs J01..J12 (T-SQL ones) and exports

Writes:
    legacy/golden/<schema>_<table>.csv   source of truth for reconciliation
    legacy/golden/sas_input/*.csv        files to upload to SAS OnDemand (exl/input/)
"""
from __future__ import annotations

import csv
import os
import re
import shutil
import sys
import time
from decimal import Decimal
from pathlib import Path

import pymssql

ROOT = Path(__file__).resolve().parents[1]
TSQL = ROOT / "legacy" / "tsql"
GOLDEN = ROOT / "legacy" / "golden"
SAS_IN = GOLDEN / "sas_input"

# Legacy run order (from the scheduler), not derived from the code.
ORDER = ["J01", "J02", "J03", "J05", "J06", "J08", "J09", "J11", "J12"]
# T-SQL outputs the SAS jobs read, exported with the names the SAS code expects.
SAS_FEEDS = {"stg.claims": "stg_claims.csv", "stg.policies": "stg_policies.csv",
             "stg.policy_earned_premium": "stg_policy_earned_premium.csv"}


def _password() -> str:
    pw = os.getenv("MSSQL_SA_PASSWORD")
    env = ROOT / ".env"
    if not pw and env.exists():
        for line in env.read_text().splitlines():
            if line.startswith("MSSQL_SA_PASSWORD="):
                pw = line.split("=", 1)[1].split("#")[0].strip()
    if not pw:
        sys.exit("Set MSSQL_SA_PASSWORD in .env")
    return pw


def _fmt(v) -> str:
    if v is None:
        return ""
    if isinstance(v, Decimal):
        return format(v, "f")
    if hasattr(v, "isoformat"):
        return v.isoformat()[:10]
    return str(v)


def export(cur, table: str, path: Path) -> int:
    cur.execute(f"SELECT * FROM {table} ORDER BY 1, 2")
    cols = [c[0] for c in cur.description]
    rows = cur.fetchall()
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(cols)
        w.writerows([[_fmt(v) for v in r] for r in rows])
    return len(rows)


def main() -> None:
    GOLDEN.mkdir(parents=True, exist_ok=True)
    SAS_IN.mkdir(parents=True, exist_ok=True)
    conn = pymssql.connect(server="localhost", port=1433, user="sa", password=_password(),
                           database="InsuranceLegacy", autocommit=True)
    cur = conn.cursor()
    files = {p.name[:3]: p for p in TSQL.glob("J*.sql")}
    for job in ORDER:
        path = files[job]
        sql = path.read_text()
        target = re.search(r"\bINTO\s+(\w+\.\w+)", sql).group(1)
        t0 = time.time()
        cur.execute(sql)
        n = export(cur, target, GOLDEN / f"{target.replace('.', '_')}.csv")
        print(f"{path.stem:34s} -> {target:28s} {n:>8} rows  {time.time() - t0:5.1f}s")
    for table, name in SAS_FEEDS.items():
        shutil.copy(GOLDEN / f"{table.replace('.', '_')}.csv", SAS_IN / name)
    shutil.copy(ROOT / "data" / "raw" / "claim_payments.csv", SAS_IN / "claim_payments.csv")
    print(f"\nSAS inputs ready in {SAS_IN.relative_to(ROOT)} -> upload to ~/exl/input in SAS OnDemand")


if __name__ == "__main__":
    main()
