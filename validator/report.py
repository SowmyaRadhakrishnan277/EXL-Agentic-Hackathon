"""
The validation report: an Excel workbook, a JSON file and a short text summary.

Excel sheets
  Summary       one row per check: PASS / FAIL, row counts, rows on one side only, cells differing
  Columns       per compared column: cells differing and the column total on each side
  Mismatches    key, column, legacy value, cloud value, difference
  Legacy_Only   rows the legacy system has and the cloud system does not
  Cloud_Only    rows the cloud system has and the legacy system does not
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

HEAD_FILL = PatternFill("solid", start_color="1F3864")
HEAD_FONT = Font(name="Arial", bold=True, color="FFFFFF", size=10)
BODY = Font(name="Arial", size=10)
FILL = {"PASS": PatternFill("solid", start_color="D9EAD3"), "FAIL": PatternFill("solid", start_color="F4CCCC"),
        "ERROR": PatternFill("solid", start_color="FCE5CD")}

SUMMARY = [("Check", "check"), ("Status", "status"), ("Legacy rows", "legacy_rows"), ("Cloud rows", "cloud_rows"),
           ("Matched rows", "matched_rows"), ("Legacy only", "legacy_only_rows"), ("Cloud only", "cloud_only_rows"),
           ("Rows differing", "rows_differing"), ("Cells differing", "cells_differing"),
           ("Duplicate keys (legacy)", "duplicate_keys_legacy"), ("Duplicate keys (cloud)", "duplicate_keys_cloud"),
           ("Key", "_keys"), ("Columns only in legacy", "_only_l"), ("Columns only in cloud", "_only_c"),
           ("Why it failed", "_problems"), ("Legacy source", "legacy_source"), ("Cloud source", "cloud_source"),
           ("Seconds", "seconds")]
COLUMNS = [("Check", "check"), ("Column", "column"), ("Rows compared", "rows_compared"),
           ("Cells differing", "cells_differing"), ("Legacy total", "legacy_total"), ("Cloud total", "cloud_total"),
           ("Total difference", "total_difference")]
MISMATCH = [("Check", "check"), ("Key", "key"), ("Column", "column"), ("Legacy value", "legacy_value"),
            ("Cloud value", "cloud_value"), ("Difference", "difference")]
ONLY = [("Check", "check"), ("Key", "key"), ("Row", "row")]


def _flat(r: dict) -> dict:
    return {**r, "_keys": ", ".join(r.get("keys") or []) or ("whole row" if r.get("keyless") else ""),
            "_only_l": ", ".join(r.get("columns_only_in_legacy") or []),
            "_only_c": ", ".join(r.get("columns_only_in_cloud") or []),
            "_problems": "; ".join(r.get("problems") or [])}


def _cell(v):
    """Text that is a whole number or a short decimal becomes a number so Excel can sum it."""
    if isinstance(v, str) and v and len(v) <= 15:
        try:
            if v.lstrip("-").isdigit() and not (len(v.lstrip("-")) > 1 and v.lstrip("-")[0] == "0"):
                return int(v)
        except ValueError:
            pass
    return v


def _sheet(ws, spec, rows, status_col=None, numeric=False):
    ws.append([h for h, _ in spec])
    for c in ws[1]:
        c.fill, c.font, c.alignment = HEAD_FILL, HEAD_FONT, Alignment(vertical="center", wrap_text=True)
    for r in rows:
        ws.append([(_cell(r.get(k, "")) if numeric else r.get(k, "")) for _, k in spec])
    for row in ws.iter_rows(min_row=2):
        for c in row:
            c.font = BODY
        if status_col is not None and row[status_col].value in FILL:
            row[status_col].fill = FILL[row[status_col].value]
    for i, (h, k) in enumerate(spec, 1):
        longest = max([len(h)] + [len(str(r.get(k, ""))) for r in rows[:200]])
        ws.column_dimensions[get_column_letter(i)].width = min(max(10, longest + 2), 70)
    ws.freeze_panes = "A2"
    if rows:
        ws.auto_filter.ref = ws.dimensions


def write_excel(results: list[dict], path: Path, title: str = "") -> Path:
    wb = Workbook()
    ws = wb.active
    ws.title = "Summary"
    _sheet(ws, SUMMARY, [_flat(r) for r in results], status_col=1)
    ok = [r for r in results if "columns" in r]
    _sheet(wb.create_sheet("Columns"), COLUMNS, [c for r in ok for c in r["columns"]], numeric=True)
    _sheet(wb.create_sheet("Mismatches"), MISMATCH, [m for r in ok for m in r["mismatches"]])
    _sheet(wb.create_sheet("Legacy_Only"), ONLY, [m for r in ok for m in r["legacy_only"]])
    _sheet(wb.create_sheet("Cloud_Only"), ONLY, [m for r in ok for m in r["cloud_only"]])
    wb.properties.title = title or "Validation report"
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)
    return path


def write_json(results: list[dict], path: Path, title: str = "", sample: int = 20) -> Path:
    """Machine-readable result for agents and CI: counts in full, at most `sample` example rows."""
    slim = []
    for r in results:
        s = {k: v for k, v in r.items() if k not in ("mismatches", "legacy_only", "cloud_only")}
        for k in ("mismatches", "legacy_only", "cloud_only"):
            if k in r:
                s[f"{k}_sample"] = r[k][:sample]
        slim.append(s)
    passed = sum(r["status"] == "PASS" for r in results)
    doc = {"title": title, "generated": datetime.now().isoformat(timespec="seconds"),
           "status": "PASS" if passed == len(results) and results else "FAIL",
           "checks": len(results), "passed": passed, "failed": len(results) - passed, "results": slim}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2, default=str))
    return path


def text(results: list[dict]) -> str:
    lines = []
    for r in results:
        if r["status"] == "ERROR":
            lines.append(f"  ERROR  {r['check']:32s} {'; '.join(r['problems'])}")
            continue
        lines.append(f"  {r['status']:5s}  {r['check']:32s} legacy {r['legacy_rows']:>9,}  cloud {r['cloud_rows']:>9,}  "
                     f"legacy-only {r['legacy_only_rows']:,}  cloud-only {r['cloud_only_rows']:,}  "
                     f"cells differing {r['cells_differing']:,}")
        for p in r["problems"]:
            lines.append(f"           - {p}")
    passed = sum(r["status"] == "PASS" for r in results)
    lines.append(f"\n  {passed} of {len(results)} checks passed")
    return "\n".join(lines)
