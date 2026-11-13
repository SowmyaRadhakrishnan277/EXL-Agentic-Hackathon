"""
Build silver and gold from bronze with the reference translations, and optionally
check every output with the factory's reconciler (data contract + legacy golden output).

    python -m factory.bronze                          # once: raw CSVs -> bronze
    python -m reference_jobs.run                      # build silver + gold
    python -m reference_jobs.run runs/<run_id>        # build + check against legacy/golden/
    python -m reference_jobs.run runs/<run_id> J05 J12

SPARK_ENGINE=databricks runs the same thing on Databricks through Databricks Connect.

Checking: each output is first conformed to its data contract (contract columns, in contract
order, cast to the contract type), then compared with the golden CSV by the same code the
migration agent uses: row counts, column totals, and a row-level diff on the primary key.
So a pass here means the VALUES match legacy, record for record.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from factory import catalog
from factory.config import ROOT, load_env


def conform(df, contract):
    from pyspark.sql import functions as F
    df = df.toDF(*[c.lower() for c in df.columns])
    if not contract.columns:          # SAS job designed before its golden file existed
        return df
    return df.select(*[F.col(c.name).cast(c.dtype).alias(c.name) for c in contract.columns if c.name in df.columns])


def main() -> int:
    load_env()
    from factory.runtime import lakehouse
    from factory.runtime.spark import engine_name, get_spark
    from reference_jobs.jobs import JOBS, run_pipeline

    args = sys.argv[1:]
    run_dir = ROOT / args.pop(0) if args and not args[0].upper().startswith("J") else None
    only = {a.upper()[:3] for a in args} or None
    spark = get_spark()
    lakehouse.ensure_schemas(spark)
    print(f"Reference jobs on {engine_name()}"
          + (f", checking against legacy/golden/ with contracts from {run_dir.name}" if run_dir else ""))

    if not run_dir:
        run_pipeline(spark, lambda t: lakehouse.read_table(spark, t), lakehouse.write_table, only)
        return 0

    from factory.agents import reconcile
    from factory.agents.design import DesignResult
    from factory.contracts.models import DiscoveryResult

    results, failed = [], 0
    for job in sorted(JOBS, key=lambda j: (j.wave, j.job_id)):
        if only and job.job_id not in only:
            continue
        spec = DesignResult.model_validate_json((run_dir / "design" / f"{job.name}.json").read_text()).spec
        disc = DiscoveryResult.model_validate_json((run_dir / "discovery" / f"{job.name}.json").read_text())
        legacy_of = {catalog.lakehouse_name(t): t for t in disc.output_tables}
        t0 = time.time()
        outputs = job.run(spark, lakehouse.load_inputs(spark, list(job.inputs)))
        outputs = {t: conform(outputs[t], c) for c in spec.contracts for t in [c.table] if t in outputs}
        report = reconcile.check(spark, job.job_id, 1, outputs, spec.contracts, legacy_of, t0)
        for c in spec.contracts:
            if c.table in outputs:
                lakehouse.write_table(outputs[c.table], c.table)
        verdict = ("MATCHES LEGACY" if report.passed and report.golden_checked
                   else "contract only (no golden file)" if report.passed else "DIFFERS")
        failed += not report.passed
        for tbl in report.tables:
            print(f"wave {job.wave}  {job.name:32s} {tbl.table:30s} legacy {tbl.legacy_rows:>8,}  "
                  f"migrated {tbl.migrated_rows:>8,}  {verdict}")
            for v in tbl.contract_violations:
                print(f"      contract: {v[:160]}")
            if tbl.column_total_diffs:
                print(f"      column totals differ: {tbl.column_total_diffs}")
            if tbl.mismatched_keys:
                print(f"      {tbl.mismatched_keys} rows differ, e.g. {tbl.sample_mismatches[:2]}")
        results.append(report.model_dump(mode="json"))

    out = run_dir / "reference"
    out.mkdir(exist_ok=True)
    (out / "summary.json").write_text(json.dumps(results, indent=2, default=str))
    print(f"\n{len(results) - failed}/{len(results)} jobs passed. Reports: {out.relative_to(ROOT)}/summary.json")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
