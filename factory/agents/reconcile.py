"""
Checks a migrated job's output. Used by the migration agent after every attempt.

1. Data contract (always): column names and order, types, NOT NULL columns,
   primary-key uniqueness.
2. Legacy output (when legacy/golden/<table>.csv exists): row counts, column totals,
   and a row-level diff joined on the primary key (or a full-row multiset diff when
   there is no key). Values are compared after casting both sides to the contract type,
   NULL-safe.

Every problem goes into a ReconciliationReport, which the translation agent receives
as feedback when it has to fix its code.
"""
from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Optional

from factory.config import ROOT
from factory.contracts.models import DataContract, ReconciliationReport, TableDiff
from factory.runtime.spark import engine_name

GOLDEN = ROOT / "legacy" / "golden"
SAMPLES = 8
NUMERIC = re.compile(r"^(int|bigint|smallint|tinyint|double|float|decimal\(\d+,\d+\))$")


def golden_path(legacy_table: str) -> Path:
    """stg.claims -> stg_claims.csv ; legacy.claim_totals -> claim_totals.csv."""
    schema, name = legacy_table.lower().split(".", 1)
    return GOLDEN / (f"{name}.csv" if schema == "legacy" else f"{schema}_{name}.csv")


def _family(dtype: str) -> str:
    d = dtype.lower().replace(" ", "")
    if d in ("int", "integer", "bigint", "smallint", "tinyint"):
        return "integer"
    if d.startswith("decimal"):
        return "decimal"
    return d


def _scale(dtype: str) -> Optional[int]:
    m = re.match(r"decimal\(\d+,(\d+)\)", dtype.lower().replace(" ", ""))
    return int(m.group(1)) if m else None


def contract_violations(df, contract: DataContract) -> list[str]:
    from pyspark.sql import functions as F
    out = []
    got = [f.name.lower() for f in df.schema.fields]
    want = [c.name for c in contract.columns]
    if got != want:
        missing = [c for c in want if c not in got]
        extra = [c for c in got if c not in want]
        out.append(f"columns differ from contract: missing {missing}, unexpected {extra}"
                   + ("" if missing or extra else f", order should be {want}"))
    types = {f.name.lower(): f.dataType.simpleString() for f in df.schema.fields}
    for c in contract.columns:
        if c.name not in types:
            continue
        g, w = types[c.name], c.dtype
        if _family(g) != _family(w):
            out.append(f"{c.name}: type {g}, contract says {w}")
        elif _family(w) == "decimal" and _scale(g) != _scale(w):
            out.append(f"{c.name}: {g} has {_scale(g)} decimal places, contract says {w}")
    present = [c for c in contract.columns if c.name in types]
    nn = [c.name for c in present if not c.nullable]
    if nn:
        row = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in nn]).first()
        out += [f"{c}: {row[c]} NULL values in a NOT NULL column" for c in nn if row[c]]
    key = [k for k in contract.primary_key if k in types]
    if key and len(key) == len(contract.primary_key):
        dups = df.groupBy(*key).count().filter("count > 1")
        n = dups.count()
        if n:
            ex = [r.asDict() for r in dups.limit(3).collect()]
            out.append(f"primary key {key} not unique: {n} duplicated keys, e.g. {ex}")
    return out


def _read_golden(spark, path: Path, contract: DataContract):
    """Legacy output CSV, every column cast to its contract type (empty or SAS '.' = NULL)."""
    from pyspark.sql import functions as F
    raw = spark.read.option("header", True).csv(str(path))      # strings; empty -> NULL
    cols = {c.lower(): c for c in raw.columns}
    sel = []
    for c in contract.columns:
        if c.name not in cols:
            continue
        v = F.col(f"`{cols[c.name]}`")
        if _family(c.dtype) == "string":
            sel.append(v.alias(c.name))
        else:
            t = F.trim(v)
            sel.append(F.when(t.isin("", "."), None).otherwise(t).cast(c.dtype).alias(c.name))
    return raw.select(*sel)


def compare_with_golden(spark, df, legacy_table: str, contract: DataContract) -> Optional[TableDiff]:
    """None when no golden output exists for this table."""
    from pyspark.sql import functions as F
    path = golden_path(legacy_table)
    if not path.exists():
        return None
    legacy = _read_golden(spark, path, contract)
    cols = [c.name for c in contract.columns if c.name in legacy.columns and c.name in df.columns]
    mig = df.select(*[F.col(c).cast(next(x.dtype for x in contract.columns if x.name == c)).alias(c)
                      for c in cols])
    legacy = legacy.select(*cols)
    diff = TableDiff(table=contract.table, legacy_rows=legacy.count(), migrated_rows=mig.count())

    numeric = [c.name for c in contract.columns if c.name in cols and NUMERIC.match(c.dtype.replace(" ", ""))]
    if numeric:
        a = legacy.select([F.sum(c).alias(c) for c in numeric]).first()
        b = mig.select([F.sum(c).alias(c) for c in numeric]).first()
        for c in numeric:
            d = float(a[c] or 0) - float(b[c] or 0)
            if abs(d) > 1e-9:
                diff.column_total_diffs[c] = round(d, 6)

    key = [k for k in contract.primary_key if k in cols]
    if key:
        values = [c for c in cols if c not in key]
        L = legacy.select(*key, *[F.col(c).alias(f"legacy_{c}") for c in values]).withColumn("_l", F.lit(1))
        M = mig.select(*key, *[F.col(c).alias(f"migrated_{c}") for c in values]).withColumn("_m", F.lit(1))
        j = L.join(M, on=key, how="full_outer")
        bad = F.col("_l").isNull() | F.col("_m").isNull()
        for c in values:
            bad = bad | ~F.col(f"legacy_{c}").eqNullSafe(F.col(f"migrated_{c}"))
        wrong = j.filter(bad)
        diff.mismatched_keys = wrong.count()
        for r in wrong.limit(SAMPLES).collect():
            d = r.asDict()
            s = {k: d[k] for k in key}
            if d["_l"] is None:
                s["problem"] = "row only in migrated output"
            elif d["_m"] is None:
                s["problem"] = "row missing from migrated output"
            else:
                for c in values:
                    if d[f"legacy_{c}"] != d[f"migrated_{c}"]:
                        s[c] = {"legacy": d[f"legacy_{c}"], "migrated": d[f"migrated_{c}"]}
            diff.sample_mismatches.append(s)
    else:
        only_l, only_m = legacy.exceptAll(mig), mig.exceptAll(legacy)
        diff.mismatched_keys = only_l.count() + only_m.count()
        diff.sample_mismatches = (
            [{"problem": "row missing from migrated output", **r.asDict()} for r in only_l.limit(SAMPLES // 2).collect()]
            + [{"problem": "row only in migrated output", **r.asDict()} for r in only_m.limit(SAMPLES // 2).collect()])
    return diff


def check(spark, job_id: str, attempt: int, outputs: dict, contracts: list[DataContract],
          legacy_tables: dict[str, str], started: float) -> ReconciliationReport:
    """outputs: {lakehouse_table: DataFrame}; legacy_tables: {lakehouse_table: legacy_table}."""
    tables, golden = [], False
    for c in contracts:
        df = outputs.get(c.table)
        if df is None:
            tables.append(TableDiff(table=c.table, legacy_rows=0, migrated_rows=0,
                                    contract_violations=[f"job did not return table {c.table}; "
                                                         f"returned {sorted(outputs)}"]))
            continue
        df = df.toDF(*[x.lower() for x in df.columns])
        violations = contract_violations(df, c)
        g = compare_with_golden(spark, df, legacy_tables[c.table], c) if not violations else None
        if g is None:
            g = TableDiff(table=c.table, legacy_rows=0, migrated_rows=df.count())
        else:
            golden = True
        g.contract_violations = violations
        tables.append(g)
    passed = all(not t.contract_violations and not t.mismatched_keys and not t.column_total_diffs
                 and (t.legacy_rows == t.migrated_rows or not golden) for t in tables)
    return ReconciliationReport(job_id=job_id, attempt=attempt, passed=passed, tables=tables,
                                runtime_seconds=round(time.time() - started, 2), engine=engine_name(),
                                golden_checked=golden)
