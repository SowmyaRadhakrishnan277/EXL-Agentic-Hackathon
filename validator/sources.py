"""
Loading one side of a comparison into a pandas DataFrame.

A side is either a file:
    {file: path/to/extract.xlsx, sheet: Sheet1}        # .xlsx .xls .csv .tsv .parquet
or a table / query on a named connection:
    {connection: legacy_sql, table: rpt.open_reserves}
    {connection: lakehouse,  query: "SELECT ... FROM workspace.gold.open_reserves"}

Connection types: sqlserver, sqlite, databricks. Passwords and tokens are never written in the
configuration: it holds the NAMES of environment variables (user_env, password_env), which are
read from the environment or the project's .env file.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import pandas as pd

from .compare import ConfigError

_TABLE = re.compile(r"^[\w\[\]`\"$.\- ]+$")
_sessions: dict = {}


def load_env(path: Path) -> None:
    """KEY=VALUE lines of a .env file into the environment (existing variables win)."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        v = re.split(r"\s+#", v, maxsplit=1)[0].strip().strip("'\"")
        os.environ.setdefault(k.strip(), v)


def _secret(conn: dict, field: str, name: str) -> str:
    var = conn.get(field)
    if not var:
        raise ConfigError(f"connection '{name}': '{field}' is missing (the NAME of an environment variable)")
    value = os.getenv(str(var))
    if not value:
        raise ConfigError(f"connection '{name}': environment variable {var} is not set (put it in .env)")
    return value


def _sql(spec: dict, where: str) -> str:
    if spec.get("query"):
        return str(spec["query"]).strip().rstrip(";")
    table = str(spec.get("table") or "")
    if not _TABLE.match(table):
        raise ConfigError(f"{where}: give 'table' (a plain table name) or 'query'")
    return f"SELECT * FROM {table}"


def _rows(cursor) -> pd.DataFrame:
    cols = [d[0] for d in cursor.description]
    return pd.DataFrame.from_records(cursor.fetchall(), columns=cols)


def _from_file(spec: dict, base: Path) -> pd.DataFrame:
    path = Path(str(spec["file"])).expanduser()
    if not path.is_absolute():                       # beside the config file first, then the current folder
        path = base / path if (base / path).exists() else Path.cwd() / path
    if not path.exists():
        raise ConfigError(f"file not found: {path}")
    ext = path.suffix.lower()
    if ext in (".xlsx", ".xlsm", ".xls"):
        return pd.read_excel(path, sheet_name=spec.get("sheet", 0), dtype=object)
    if ext in (".csv", ".tsv", ".txt"):
        return pd.read_csv(path, dtype=str, keep_default_na=False, sep=spec.get("sep") or ("\t" if ext == ".tsv" else ","),
                           encoding=spec.get("encoding", "utf-8-sig"))
    if ext == ".parquet" or path.is_dir():
        return pd.read_parquet(path)
    raise ConfigError(f"{path.name}: unsupported file type (use .xlsx, .xls, .csv, .tsv or .parquet)")


def _databricks():
    if "spark" not in _sessions:
        try:
            from databricks.connect import DatabricksSession
        except ImportError as e:
            raise ConfigError("the databricks connection needs the databricks-connect package") from e
        _sessions["spark"] = DatabricksSession.builder.serverless(True).getOrCreate()
    return _sessions["spark"]


def _from_connection(spec: dict, connections: dict) -> pd.DataFrame:
    name = str(spec["connection"])
    conn = connections.get(name)
    if not isinstance(conn, dict):
        raise ConfigError(f"connection '{name}' is not defined under 'connections'")
    kind = str(conn.get("type", "")).lower()
    sql = _sql(spec, f"connection '{name}'")
    if kind == "databricks":
        return _databricks().sql(sql).toPandas()
    if kind == "sqlite":
        import sqlite3
        with sqlite3.connect(str(Path(str(conn["path"])).expanduser())) as db:
            return _rows(db.execute(sql))
    if kind == "sqlserver":
        try:
            import pymssql
        except ImportError as e:
            raise ConfigError("the sqlserver connection needs the pymssql package") from e
        db = pymssql.connect(server=str(conn.get("host", "localhost")), port=int(conn.get("port", 1433)),
                             user=str(conn["user"]) if conn.get("user") else _secret(conn, "user_env", name),
                             password=_secret(conn, "password_env", name),
                             database=str(conn.get("database", "")), login_timeout=int(conn.get("timeout", 30)))
        try:
            cur = db.cursor()
            cur.execute(sql)
            return _rows(cur)
        finally:
            db.close()
    raise ConfigError(f"connection '{name}': type must be sqlserver, sqlite or databricks (got '{kind}')")


def load(spec: dict, connections: dict, base: Path) -> pd.DataFrame:
    if not isinstance(spec, dict) or not (spec.get("file") or spec.get("connection")):
        raise ConfigError(f"a side needs 'file' or 'connection' (got {spec!r})")
    return _from_file(spec, base) if spec.get("file") else _from_connection(spec, connections)


def describe(spec: dict, connections: dict) -> str:
    """Where a side came from, for the report. Never includes a password or token."""
    if spec.get("file"):
        return str(spec["file"]) + (f" [{spec['sheet']}]" if spec.get("sheet") is not None else "")
    conn = connections.get(str(spec.get("connection")), {})
    kind = conn.get("type", "?")
    at = {"sqlserver": f"{conn.get('host', 'localhost')}/{conn.get('database', '')}",
          "sqlite": str(conn.get("path", "")), "databricks": "serverless"}.get(str(kind).lower(), "")
    what = spec.get("table") or "query: " + " ".join(str(spec.get("query", "")).split())[:80]
    return f"{kind}://{at}  {what}"
