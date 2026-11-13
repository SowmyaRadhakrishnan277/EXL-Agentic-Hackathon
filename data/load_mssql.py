"""
Load data/raw/*.csv into SQL Server running in Docker (docker/docker-compose.yml).

    docker compose -f docker/docker-compose.yml --env-file .env up -d
    python -m data.load_mssql

Works on macOS, Windows and Linux: it copies the CSVs into the container and runs
sqlcmd + BULK INSERT there, so no ODBC driver is needed on the laptop.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
DDL = ROOT / "data" / "ddl" / "create_tables.sql"
TABLES = ["products", "brokers", "customers", "policies", "premiums", "claims", "claim_payments"]
SQLCMD = "/opt/mssql-tools18/bin/sqlcmd"


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


def _container() -> str:
    out = subprocess.run(["docker", "ps", "--filter", "ancestor=mcr.microsoft.com/mssql/server:2022-latest",
                          "--format", "{{.Names}}"], capture_output=True, text=True, check=True).stdout.split()
    if not out:
        sys.exit("SQL Server container not running. Start it with:\n"
                 "  docker compose -f docker/docker-compose.yml --env-file .env up -d")
    return out[0]


def _sqlcmd(c: str, pw: str, args: list[str]) -> str:
    cmd = ["docker", "exec", c, SQLCMD, "-S", "localhost", "-U", "sa", "-P", pw, "-C", "-b", *args]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"sqlcmd failed:\n{r.stdout}\n{r.stderr}")
    return r.stdout


def main() -> None:
    pw, c = _password(), _container()
    for _ in range(30):  # wait for the server to accept logins
        if subprocess.run(["docker", "exec", c, SQLCMD, "-S", "localhost", "-U", "sa", "-P", pw, "-C",
                           "-Q", "SELECT 1"], capture_output=True).returncode == 0:
            break
        time.sleep(2)
    subprocess.run(["docker", "exec", c, "mkdir", "-p", "/tmp/load"], check=True)
    subprocess.run(["docker", "cp", str(DDL), f"{c}:/tmp/load/create_tables.sql"], check=True)
    _sqlcmd(c, pw, ["-i", "/tmp/load/create_tables.sql"])
    for t in TABLES:
        subprocess.run(["docker", "cp", str(RAW / f"{t}.csv"), f"{c}:/tmp/load/{t}.csv"], check=True)
        sql = (f"BULK INSERT InsuranceLegacy.dbo.{t} FROM '/tmp/load/{t}.csv' "
               "WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', "
               "KEEPNULLS, TABLOCK);")
        _sqlcmd(c, pw, ["-Q", sql])
        n = _sqlcmd(c, pw, ["-h", "-1", "-W", "-Q", f"SET NOCOUNT ON; SELECT COUNT(*) FROM InsuranceLegacy.dbo.{t}"])
        print(f"{t:15s} {n.strip():>8s} rows")
    print("Loaded into database InsuranceLegacy.")


if __name__ == "__main__":
    main()
