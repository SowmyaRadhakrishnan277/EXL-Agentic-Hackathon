"""
Score a discovery run against eval/answer_key.yaml.

    python -m eval.score_discovery runs/<run_id>

Prints a per-job table and headline numbers, and writes runs/<run_id>/discovery/score.json.
Column lineage is "exact" when the agent's source-column set equals the answer key's.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml

from factory.config import ROOT

KEY = ROOT / "eval" / "answer_key.yaml"


def score(run_dir: Path) -> dict:
    key = yaml.safe_load(KEY.read_text())
    d = run_dir / "discovery"
    rows, tot = [], {"io": [0, 0], "deps": [0, 0], "wave": [0, 0], "cols": [0, 0], "missing": 0}
    misses = []
    for jid, k in key.items():
        f = d / f"{jid}.json"
        if not f.exists():
            rows.append((jid, "missing")); continue
        r = json.loads(f.read_text())
        io_ok = set(r["input_tables"]) == set(k["inputs"]) and set(r["output_tables"]) == set(k["outputs"])
        dep_ok = set(r["depends_on"]) == set(k["depends_on"])
        wave_ok = r["wave"] == k["wave"]
        got = {c["target_column"]: set(c["source_columns"]) for c in r["column_lineage"]}
        exact = scored = 0
        for col, want in k["columns"].items():
            if isinstance(want, dict) and want.get("skip"):
                continue
            scored += 1
            if col not in got:
                tot["missing"] += 1
                misses.append(f"{jid[:3]}.{col}: not found")
            elif got[col] == set(want):
                exact += 1
            else:
                misses.append(f"{jid[:3]}.{col}: got {sorted(got[col])}, want {sorted(want)}")
        for name, ok in [("io", io_ok), ("deps", dep_ok), ("wave", wave_ok)]:
            tot[name][0] += ok; tot[name][1] += 1
        tot["cols"][0] += exact; tot["cols"][1] += scored
        rows.append((jid, r.get("method", "?"), io_ok, dep_ok, wave_ok, exact, scored))

    print(f"{'job':32s} {'method':14s} tables deps wave  lineage")
    for row in rows:
        if row[1] == "missing":
            print(f"{row[0]:32s} MISSING"); continue
        jid, m, a, b, c, e, s = row
        tick = lambda x: " ok " if x else " XX "
        print(f"{jid:32s} {m:14s} {tick(a)}  {tick(b)} {tick(c)}  {e:2d}/{s:<2d}")
    pct = lambda p: f"{100 * p[0] / p[1]:.0f}% ({p[0]}/{p[1]})" if p[1] else "n/a"
    summary = {"tables": pct(tot["io"]), "dependencies": pct(tot["deps"]),
               "waves": pct(tot["wave"]), "column_lineage_exact": pct(tot["cols"]),
               "columns_not_found": tot["missing"], "mismatches": misses}
    print(f"\nTables {summary['tables']} | Dependencies {summary['dependencies']} | "
          f"Waves {summary['waves']} | Column lineage {summary['column_lineage_exact']}")
    if misses:
        print("\nMismatches:\n  " + "\n  ".join(misses))
    (d / "score.json").write_text(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    score(ROOT / sys.argv[1])
