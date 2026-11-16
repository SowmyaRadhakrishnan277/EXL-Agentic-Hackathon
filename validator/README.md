# Validator: legacy system vs cloud system

Compares the same data in two places and writes a validation report. Each side can be:

- an extract file: Excel (`.xlsx`, `.xls`), CSV, TSV or Parquet
- a SQL Server table or query
- a Databricks table or query (through Databricks Connect)

It is a separate module. Nothing in `factory/` imports it, and it changes no table and no
file except its own report.

## Run it

```bash
# 1. Two files, no configuration needed
python -m validator --legacy legacy.xlsx --cloud cloud.xlsx --keys policy_id
python -m validator --legacy a.csv --cloud b.xlsx --cloud-sheet Report --keys product_code,year

# 2. Checks described in a YAML (or JSON) file: servers, queries, tolerances
python -m validator validator/example_config.yml
python -m validator validator/example_config.yml --only open_reserves

# 3. Every legacy output of a factory run against the lakehouse table the migrated job wrote
SPARK_ENGINE=databricks python -m validator --run runs/<run_id>
```

Leave `--keys` out to compare whole rows (useful when a table has no key).

Reports go to `runs/validation/<timestamp>/` (`--out DIR` to choose):

| File | Contents |
|---|---|
| `validation_report.xlsx` | Sheets: Summary, Columns, Mismatches, Legacy_Only, Cloud_Only |
| `validation_result.json` | The same result for scripts, agents and CI |

Exit code: `0` every check passed, `1` at least one failed, `2` a check could not run
(bad configuration, file not found, connection refused).

## Server details

Connections are named once in the configuration and used by any check. See
`example_config.yml` for a complete file.

```yaml
connections:
  legacy_sql:
    type: sqlserver
    host: localhost
    port: 1433
    database: InsuranceLegacy
    user: sa                          # or user_env: NAME_OF_VARIABLE
    password_env: MSSQL_SA_PASSWORD   # the NAME of an environment variable, never the password
  lakehouse:
    type: databricks                  # reads DATABRICKS_HOST and DATABRICKS_TOKEN

checks:
  - name: stg_claims
    legacy: {connection: legacy_sql, table: stg.claims}
    cloud:  {connection: lakehouse, table: workspace.silver.claims}
    keys: [claim_id]
```

Passwords and tokens are read from the environment or from the project's `.env` file. They are
never written in the configuration and never appear in a report.

## How rows and values are compared

| Rule | Behaviour |
|---|---|
| Column names | Matched ignoring case and surrounding spaces. `column_map` renames legacy columns first. `ignore` leaves columns out. |
| Row matching | On the key columns, never on row order. `101`, `101.0` and `'101'` are the same key; `'00123'` stays text. NULL keys match NULL keys. |
| NULL | NULL equals NULL. Empty text, `.` and `NULL` count as NULL (`null_values`). |
| Numbers | Compared as numbers whatever their type or formatting: `100.50` = `100.5`. Tolerance: `abs` and `rel`, or `round` to a number of decimals first. |
| Dates | Dates and timestamps at midnight are compared as dates. `date_format` reads text dates that are not ISO. |
| Text | Trimmed and case-sensitive by default (`trim`, `case_sensitive`). |
| Also reported | Columns on one side only, duplicate keys, and each numeric column's total on both sides. |

A check fails when columns differ, a key is duplicated, a row exists on one side only, or any
cell differs. Per check, `allow_schema_diff`, `allow_duplicate_keys`, `max_cells_differing` and
`max_rows_missing` relax this.

## Limits

- Both sides are loaded into memory with pandas, so it suits tables up to a few million rows.
- The detail sheets list at most 5,000 rows per check (`max_detail`); the counts are always complete.
- The SQL Server connection uses `pymssql`. It is covered by tests only through SQLite, which
  shares the same query path; it has not been run against a live SQL Server.
