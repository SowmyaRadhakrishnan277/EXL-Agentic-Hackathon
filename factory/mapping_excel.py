"""
Excel source-to-target mapping, generated from a run's agent output.

    python -m factory.mapping_doc runs/<run_id>    # writes the Markdown and this Excel

It is also written automatically at the end of the design agent and the migration
agent, so docs/source_to_target_mapping.xlsx always reflects the latest run.

Sheets
  Overview           layer rules, table / column counts per layer, job status counts
  Table Mapping      legacy table -> lakehouse table, layer, job, wave, key, Unity Catalog name
  Job Mapping        what each job reads and writes, wave, dependencies
  Column Mapping     every target column: type, nullable, key, source columns, legacy logic
  Migration Status   (once the migration agent has run) per job: status, attempts, legacy match
"""
from __future__ import annotations

import json
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from factory import catalog
from factory.config import ROOT
from factory.contracts.models import DiscoveryResult

OUT = ROOT / "docs" / "source_to_target_mapping.xlsx"

FONT = "Arial"
HEADER_FILL = PatternFill("solid", start_color="1F3864")
HEADER_FONT = Font(name=FONT, bold=True, color="FFFFFF", size=10)
BODY = Font(name=FONT, size=10)
BOLD = Font(name=FONT, size=10, bold=True)
LAYER_FILL = {"bronze": "F4E3D7", "silver": "E7E9EC", "gold": "FFF2CC"}
STATUS_FILL = {"matched": "D9EAD3", "repaired": "D9EAD3", "approved": "D9EAD3", "validated": "FFF2CC",
               "needs_review": "F4CCCC", "rejected": "F4CCCC", "blocked": "EFEFEF"}
_thin = Side(style="thin", color="D0D0D0")
BORDER = Border(left=_thin, right=_thin, top=_thin, bottom=_thin)

LAYER_RULES = [
    ("bronze", "Raw extracts, as-is, typed from the legacy DDL, plus _ingested_at and _source_file",
     "dbo.*", "dbo.<t> → bronze.<t>"),
    ("silver", "Cleaned, conformed and intermediate tables", "stg.*, SAS staging",
     "stg.<t>, SAS stg_<t> / <t> → silver.<t>"),
    ("gold", "Report-ready tables for Finance, Actuarial, Reserving, Credit control", "rpt.*, SAS reports",
     "rpt.<t>, SAS rpt_<t> → gold.<t>"),
]
LINEAGE_SOURCES = [
    ("Parser (sqlglot)", "T-SQL column lineage read directly from the code (exact)"),
    ("AI (Gemini)", "SAS column lineage mapped by the LLM; invented columns are dropped by a guardrail"),
    ("Columns from legacy output; lineage pending",
     "SAS output whose columns come from the legacy output file; source columns are filled after a "
     "discovery run with the LLM"),
    ("Source DDL", "Bronze columns; types from the legacy table definitions"),
]


# ============================================================== data

def _lang(j: DiscoveryResult) -> str:
    return "SAS" if j.method.startswith("rules") else "T-SQL"


def table_rows(jobs, designs, raw_cols) -> list[list]:
    rows = []
    for t in sorted({t for j in jobs for t in j.input_tables if t.startswith("dbo.")}):
        lt = catalog.lakehouse_name(t)
        rows.append([t, lt, "bronze", "Bronze loader", "CSV extract", 0,
                     raw_cols.get(t, [""])[0], catalog.uc_name(lt)])
    for j in jobs:
        d = designs.get(j.job_id)
        for t in j.output_tables:
            lt = catalog.lakehouse_name(t)
            c = next((c for c in (d.spec.contracts if d else []) if c.table == lt), None)
            rows.append([t, lt, catalog.layer_and_name(t)[0].value, j.job_id, _lang(j), j.wave,
                         ", ".join(c.primary_key) if c else "", catalog.uc_name(lt)])
    return rows


def job_rows(jobs) -> list[list]:
    return [[j.wave, j.job_id, _lang(j), ", ".join(j.input_tables), ", ".join(j.output_tables),
             ", ".join(catalog.lakehouse_name(t) for t in j.input_tables),
             ", ".join(catalog.lakehouse_name(t) for t in j.output_tables),
             ", ".join(x[:3] for x in j.depends_on) or "—"] for j in jobs]


def column_rows(jobs, designs, raw_cols, raw_types) -> list[list]:
    rows = []
    for t in sorted({t for j in jobs for t in j.input_tables if t.startswith("dbo.")}):
        lt = catalog.lakehouse_name(t)
        for col in raw_cols.get(t, []):
            dtype, nullable = raw_types[f"{t}.{col}"]
            rows.append(["Bronze loader", "CSV extract", t, lt, "bronze", col, dtype,
                         "Yes" if nullable else "No", "Yes" if col == raw_cols[t][0] else "",
                         f"{t}.{col}", "Loaded as-is (no cleaning)", "Source DDL"])
    for j in jobs:
        d = designs.get(j.job_id)
        for t in j.output_tables:
            lt = catalog.lakehouse_name(t)
            c = next((c for c in (d.spec.contracts if d else []) if c.table == lt), None)
            lin = {x.target_column: x for x in j.column_lineage if x.target_table == t}
            for col in (c.columns if c else []):
                x = lin.get(col.name)
                if x:
                    srcs = (", ".join(catalog.lakehouse_column(s) for s in x.source_columns)
                            or "— (count / generated)")
                    logic = " ".join(x.transformation.split())
                    how = "Parser (sqlglot)" if _lang(j) == "T-SQL" else "AI (Gemini)"
                else:
                    srcs, how = "Pending", "Columns from legacy output; lineage pending"
                    logic = "Pending: SAS column lineage needs a discovery run with the LLM"
                rows.append([j.job_id, _lang(j), t, lt, catalog.layer_and_name(t)[0].value, col.name,
                             col.dtype, "Yes" if col.nullable else "No",
                             "Yes" if col.name in c.primary_key else "", srcs, logic, how])
    return rows


def status_rows(run_dir: Path, jobs, designs) -> list[list]:
    mig = run_dir / "migrate"
    rows = []
    for j in jobs:
        f = mig / f"{j.job_id}.json"
        if not f.exists():
            continue
        data = json.loads(f.read_text())
        rec, reports = data["record"], data.get("reports", [])
        last = reports[-1] if reports else None
        tables = last["tables"] if last else []
        issue = ""
        if last and not last["passed"]:
            v = next((x for t in tables for x in t["contract_violations"]), "")
            lines = [s for s in v.splitlines() if s.strip()]
            issue = (lines[-1] if lines else
                     f"{sum(t['mismatched_keys'] for t in tables)} rows differ from legacy output")
        elif rec["status"] == "blocked":
            issue = "; ".join(data.get("notes", []))
        d = designs.get(j.job_id)
        rows.append([
            j.wave, j.job_id, _lang(j), d.spec.layer.value if d else "",
            ", ".join(d.spec.target_tables) if d else "", rec["status"], rec["attempts"],
            ("Legacy output" if last["golden_checked"] else "Data contract only") if last else "Not run",
            sum(t["legacy_rows"] for t in tables) if last and last["golden_checked"] else None,
            sum(t["migrated_rows"] for t in tables) if last else None,
            sum(t["mismatched_keys"] for t in tables) if last and last["golden_checked"] else None,
            rec["agent_seconds"], issue[:300]])
    return rows


# ============================================================== writing

def _sheet(ws, headers, rows, widths, fill_col=None, fills=LAYER_FILL):
    ws.append(headers)
    for c in ws[1]:
        c.font, c.fill, c.border = HEADER_FONT, HEADER_FILL, BORDER
        c.alignment = Alignment(vertical="center", wrap_text=True)
    for r in rows:
        ws.append(r)
    for row in ws.iter_rows(min_row=2):
        key = row[fill_col].value if fill_col is not None else None
        fill = PatternFill("solid", start_color=fills[key]) if key in fills else None
        for c in row:
            c.font, c.border = BODY, BORDER
            c.alignment = Alignment(vertical="top", wrap_text=True)
            if fill:
                c.fill = fill
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    ws.row_dimensions[1].height = 30


def _overview(ws, run_name: str, has_status: bool) -> None:
    ws["A1"] = "Source-to-Target Mapping: Legacy SQL Server / SAS → Lakehouse"
    ws["A1"].font = Font(name=FONT, bold=True, size=14)
    ws["A2"] = ("EXL × UCD Data & AI Hackathon 2026, Challenge 03. Generated by the migration factory "
                f"from run {run_name}; do not edit by hand. Synthetic data only.")
    ws["A2"].font = Font(name=FONT, size=10, italic=True)
    ws.append([])
    ws.append(["Layer", "Holds", "Legacy source", "Naming rule", "Tables", "Columns"])
    for c in ws[4]:
        c.font, c.fill, c.border = HEADER_FONT, HEADER_FILL, BORDER
    for i, (layer, holds, src, rule) in enumerate(LAYER_RULES, start=5):
        ws.append([layer, holds, src, rule, f"=COUNTIF('Table Mapping'!$C:$C,A{i})",
                   f"=COUNTIF('Column Mapping'!$E:$E,A{i})"])
    ws.append(["Total", "", "", "", "=SUM(E5:E7)", "=SUM(F5:F7)"])
    for row in ws.iter_rows(min_row=5, max_row=8):
        for c in row:
            c.font, c.border = BODY, BORDER
            c.alignment = Alignment(vertical="top", wrap_text=True)
        if row[0].value in LAYER_FILL:
            for c in row:
                c.fill = PatternFill("solid", start_color=LAYER_FILL[row[0].value])
    for c in ws[8]:
        c.font = BOLD

    r = 10
    ws.cell(r, 1, "Lineage source").font = BOLD
    for label, desc in LINEAGE_SOURCES:
        r += 1
        ws.cell(r, 1, label).font = BOLD
        ws.cell(r, 2, desc).font = BODY
        ws.cell(r, 2).alignment = Alignment(wrap_text=True, vertical="top")

    if has_status:
        r += 2
        ws.cell(r, 1, "Migration status").font = BOLD
        ws.cell(r, 2, "Jobs").font = BOLD
        first = r + 1
        for s in ["matched", "repaired", "validated", "needs_review", "blocked"]:
            r += 1
            ws.cell(r, 1, s).font = BODY
            ws.cell(r, 2, f"=COUNTIF('Migration Status'!$F:$F,A{r})").font = BODY
            ws.cell(r, 2).alignment = Alignment(horizontal="left")
            if s in STATUS_FILL:
                ws.cell(r, 1).fill = PatternFill("solid", start_color=STATUS_FILL[s])
        r += 1
        ws.cell(r, 1, "Total").font = BOLD
        ws.cell(r, 2, f"=SUM(B{first}:B{r - 1})").font = BOLD
        ws.cell(r, 2).alignment = Alignment(horizontal="left")

    for col, w in zip("ABCDEF", [26, 62, 20, 36, 10, 10]):
        ws.column_dimensions[col].width = w


def write(run_dir: Path, out: Path = OUT) -> Path:
    from factory.agents.design import DesignResult, source_columns_in_order, source_types
    disc = {f.stem: DiscoveryResult.model_validate_json(f.read_text())
            for f in (run_dir / "discovery").glob("J*.json")}
    designs = {f.stem: DesignResult.model_validate_json(f.read_text())
               for f in (run_dir / "design").glob("J*.json")}
    if not disc or not designs:
        raise FileNotFoundError(f"{run_dir} has no discovery/ and design/ output to map")
    jobs = sorted(disc.values(), key=lambda r: (r.wave or 0, r.job_id))
    raw_cols, raw_types = source_columns_in_order(), source_types()
    status = status_rows(run_dir, jobs, designs)

    wb = Workbook()
    ws = wb.active
    ws.title = "Overview"
    _overview(ws, run_dir.name, bool(status))
    _sheet(wb.create_sheet("Table Mapping"),
           ["Legacy table", "Lakehouse table", "Layer", "Built by", "Language", "Wave", "Primary key",
            "Unity Catalog name"],
           table_rows(jobs, designs, raw_cols), [28, 30, 9, 30, 12, 7, 30, 36], fill_col=2)
    _sheet(wb.create_sheet("Job Mapping"),
           ["Wave", "Job", "Language", "Reads (legacy)", "Writes (legacy)", "Reads (lakehouse)",
            "Writes (lakehouse)", "Depends on"],
           job_rows(jobs), [7, 30, 10, 40, 28, 44, 30, 14])
    _sheet(wb.create_sheet("Column Mapping"),
           ["Job", "Language", "Legacy table", "Lakehouse table", "Layer", "Target column", "Data type",
            "Nullable", "Primary key", "Source column(s)", "Legacy logic", "Lineage source"],
           column_rows(jobs, designs, raw_cols, raw_types),
           [28, 11, 26, 28, 9, 22, 15, 9, 9, 44, 60, 26], fill_col=4)
    if status:
        ms = wb.create_sheet("Migration Status")
        _sheet(ms, ["Wave", "Job", "Language", "Layer", "Target table(s)", "Status", "Attempts",
                    "Checked against", "Legacy rows", "Migrated rows", "Rows differing", "Agent seconds",
                    "Last issue"],
               status, [7, 30, 10, 9, 30, 14, 9, 18, 12, 13, 12, 12, 60], fill_col=5, fills=STATUS_FILL)
        for row in ms.iter_rows(min_row=2, min_col=9, max_col=11):
            for c in row:
                c.number_format = "#,##0"
        for c in ms["L"][1:]:
            c.number_format = "0.0"
    out.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out)
    return out


def refresh(run_dir: Path) -> None:
    """Called at the end of the design and migration agents; never fails the run."""
    try:
        print(f"  mapping workbook -> {write(run_dir).relative_to(ROOT)}")
    except Exception as e:  # noqa: BLE001
        print(f"  warning: could not write the mapping workbook: {e}")
