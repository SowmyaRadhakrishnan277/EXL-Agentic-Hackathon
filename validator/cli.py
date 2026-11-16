"""
Command line of the validator. See README.md in this folder.

    python -m validator validator/example_config.yml          # checks described in a YAML / JSON file
    python -m validator --legacy a.xlsx --cloud b.xlsx --keys policy_id
    python -m validator --run runs/<run_id>                   # every legacy output vs its lakehouse table

Exit code: 0 every check passed, 1 at least one check failed, 2 a check could not be run.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from . import report, sources
from .compare import ColumnRule, ConfigError, Rules, compare

ROOT = Path(__file__).resolve().parent.parent
RULE_FIELDS = ("abs", "rel", "round", "trim", "case_sensitive", "date_format", "null_values")


def _rule(base: ColumnRule, spec: dict, where: str) -> ColumnRule:
    spec = dict(spec or {})
    tol = spec.pop("tolerance", None) or {}
    spec = {**tol, **spec}
    unknown = [k for k in spec if k not in RULE_FIELDS]
    if unknown:
        raise ConfigError(f"{where}: unknown setting(s) {unknown}; allowed: {list(RULE_FIELDS)}")
    if "null_values" in spec:
        spec["null_values"] = tuple(str(v).strip().lower() for v in spec["null_values"])
    return ColumnRule(**{**base.__dict__, **spec})


def read_config(path: Path) -> dict:
    if not path.exists():
        raise ConfigError(f"configuration file not found: {path}")
    if path.suffix.lower() == ".json":
        cfg = json.loads(path.read_text())
    else:
        import yaml
        cfg = yaml.safe_load(path.read_text())
    if not isinstance(cfg, dict) or not isinstance(cfg.get("checks"), list) or not cfg["checks"]:
        raise ConfigError(f"{path.name}: needs a 'checks' list with at least one check")
    return cfg


def run_checks(cfg: dict, base: Path, only: set[str] | None = None, log=print) -> list[dict]:
    connections = cfg.get("connections") or {}
    defaults = cfg.get("defaults") or {}
    default_rule = _rule(ColumnRule(), {k: v for k, v in defaults.items() if k in RULE_FIELDS + ("tolerance",)}, "defaults")
    results = []
    for i, chk in enumerate(cfg["checks"], 1):
        name = str(chk.get("name") or f"check_{i}")
        if only and not any(name == o or name.upper().startswith(o.upper()) for o in only):
            continue
        try:
            for side in ("legacy", "cloud"):
                if side not in chk:
                    raise ConfigError(f"{name}: '{side}' is missing")
            rules = Rules(default_rule, {str(c).strip().lower(): _rule(default_rule, spec, f"{name}.columns.{c}")
                                         for c, spec in (chk.get("columns") or {}).items()})
            legacy = sources.load(chk["legacy"], connections, base)
            cloud = sources.load(chk["cloud"], connections, base)
            res = compare(name, legacy, cloud, keys=chk.get("keys"), rules=rules,
                          column_map=chk.get("column_map"), ignore=chk.get("ignore"),
                          max_detail=int(chk.get("max_detail", defaults.get("max_detail", 5000))),
                          allow_schema_diff=bool(chk.get("allow_schema_diff", defaults.get("allow_schema_diff", False))),
                          allow_duplicate_keys=bool(chk.get("allow_duplicate_keys", defaults.get("allow_duplicate_keys", False))),
                          max_cells_differing=int(chk.get("max_cells_differing", 0)),
                          max_rows_missing=int(chk.get("max_rows_missing", 0)))
        except Exception as e:                        # this check cannot run; the others still do
            kind = "" if isinstance(e, ConfigError) else f"{type(e).__name__}: "
            res = {"check": name, "status": "ERROR", "problems": [kind + " ".join(str(e).split())[:300]]}
        res["legacy_source"] = sources.describe(chk.get("legacy") or {}, connections)
        res["cloud_source"] = sources.describe(chk.get("cloud") or {}, connections)
        results.append(res)
        log(report.text([res]).splitlines()[0])
        for p in res["problems"] if res["status"] != "ERROR" else []:
            log(f"           - {p}")
    return results


def config_from_run(run_dir: Path) -> dict:
    """One check per legacy output table of a factory run: legacy/golden/<table>.csv against the
    lakehouse table the migrated job wrote, keyed and rounded as the table's data contract says."""
    from factory import catalog                       # read-only use of the factory's naming rules
    from factory.agents.reconcile import golden_path
    if not (run_dir / "design").is_dir():
        raise ConfigError(f"{run_dir} is not a factory run folder (no design/ inside)")
    on_databricks = os.getenv("SPARK_ENGINE") == "databricks"
    checks = []
    jobs = sorted(((json.loads(f.read_text()), f.stem) for f in (run_dir / "discovery").glob("J*.json")),
                  key=lambda x: (x[0].get("wave") or 0, x[1]))
    for disc, job_id in jobs:
        design = json.loads((run_dir / "design" / f"{job_id}.json").read_text())["spec"]
        contracts = {c["table"]: c for c in design.get("contracts", [])}
        for legacy_table in disc.get("output_tables", []):
            lake = catalog.lakehouse_name(legacy_table)
            golden = golden_path(legacy_table)
            if not golden.exists():
                continue
            contract = contracts.get(lake, {})
            cols = {}
            for c in contract.get("columns", []):
                m = re.match(r"decimal\(\d+,\s*(\d+)\)", str(c.get("dtype", "")))
                if m:
                    cols[c["name"]] = {"round": int(m.group(1))}
            cloud = ({"connection": "lakehouse", "table": catalog.uc_name(lake)} if on_databricks
                     else {"file": str(ROOT / "lakehouse" / lake.replace(".", "/"))})
            checks.append({"name": f"{job_id[:3]} {lake}", "legacy": {"file": str(golden)}, "cloud": cloud,
                           "keys": contract.get("primary_key") or [], "columns": cols})
    if not checks:
        raise ConfigError(f"{run_dir}: no legacy output files found under legacy/golden/")
    return {"title": f"Legacy output vs lakehouse, run {run_dir.name}",
            "connections": {"lakehouse": {"type": "databricks"}}, "checks": checks}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m validator", description="Validate one system's data against another's.")
    ap.add_argument("config", nargs="?", help="YAML or JSON file describing the connections and checks")
    ap.add_argument("--legacy", help="quick mode: legacy extract (.xlsx .xls .csv .parquet)")
    ap.add_argument("--cloud", help="quick mode: cloud extract (.xlsx .xls .csv .parquet)")
    ap.add_argument("--keys", default="", help="quick mode: key column(s), comma separated (none = compare whole rows)")
    ap.add_argument("--legacy-sheet", help="quick mode: sheet name in the legacy workbook")
    ap.add_argument("--cloud-sheet", help="quick mode: sheet name in the cloud workbook")
    ap.add_argument("--run", help="factory run folder: check every legacy output against its lakehouse table")
    ap.add_argument("--only", nargs="*", help="run only these checks (names or name prefixes, e.g. J05)")
    ap.add_argument("--out", help="folder for the report (default: runs/validation/<timestamp>)")
    args = ap.parse_args(argv)

    sources.load_env(ROOT / ".env")
    try:
        if args.run:
            run_dir = Path(args.run) if Path(args.run).is_absolute() else ROOT / args.run
            cfg, base = config_from_run(run_dir), ROOT
        elif args.legacy or args.cloud:
            if not (args.legacy and args.cloud):
                raise ConfigError("quick mode needs both --legacy and --cloud")
            side = lambda f, sheet: {"file": f, **({"sheet": sheet} if sheet else {})}
            cfg = {"title": f"{Path(args.legacy).name} vs {Path(args.cloud).name}",
                   "checks": [{"name": Path(args.legacy).stem, "legacy": side(args.legacy, args.legacy_sheet),
                               "cloud": side(args.cloud, args.cloud_sheet),
                               "keys": [k.strip() for k in args.keys.split(",") if k.strip()]}]}
            base = Path.cwd()
        elif args.config:
            path = Path(args.config).expanduser()
            cfg, base = read_config(path), path.resolve().parent
        else:
            ap.print_help()
            return 2
        title = str(cfg.get("title") or "Validation")
        print(f"{title}  ({len(cfg['checks'])} check{'s' if len(cfg['checks']) != 1 else ''})")
        results = run_checks(cfg, base, set(args.only) if args.only else None)
        if not results:
            raise ConfigError("no check matched --only")
    except ConfigError as e:
        print(f"validator: {e}", file=sys.stderr)
        return 2

    out = Path(args.out).expanduser() if args.out else ROOT / "runs" / "validation" / datetime.now().strftime("%Y%m%d-%H%M%S")
    xlsx = report.write_excel(results, out / "validation_report.xlsx", title)
    js = report.write_json(results, out / "validation_result.json", title)
    print(report.text(results).splitlines()[-1])
    show = lambda p: p.relative_to(ROOT) if p.is_relative_to(ROOT) else p
    print(f"  report: {show(xlsx)}\n  result: {show(js)}")
    if any(r["status"] == "ERROR" for r in results):
        return 2
    return 0 if all(r["status"] == "PASS" for r in results) else 1
