"""
Review dashboard (stage 5): one self-contained HTML page per run showing how every legacy
job was converted.

  * headline: how many jobs reconcile with the legacy output, first try vs after self-repair
  * waves: the dependency order the agent ran the jobs in, coloured by result
  * per job: status, attempts, row counts legacy vs migrated, the converted PySpark next to
    the legacy source, why each attempt failed, and every note the agents left
    (discovery risks, design guardrails, translation assumptions)

Reads runs/<run_id>/{discovery,design,migrate}/, legacy/ and migrated/; writes
runs/<run_id>/dashboard.html, a single file with no external data. It works for a run that is
still going or was stopped: jobs with no result yet are shown as "Not run yet".

    python -m factory.agents.dashboard                  # latest run: build the page and open it in the browser
    python -m factory.agents.dashboard runs/<run_id>    # a named run
    python -m factory.agents.dashboard --serve          # live at http://localhost:8765: every refresh
                                                        # re-reads the latest run (watch a run in progress)
    ... --no-open        # do not open the browser
    ... --port=9000      # with --serve: another port

The migrate agent builds and opens the same page when a run finishes (factory.agents.migrate).
"""
from __future__ import annotations

import html
import json
import re
import sys
import webbrowser
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional

from factory.config import ROOT

PASSED = ("matched", "repaired", "validated", "approved")


def _why(report: dict) -> str:
    """One-line reason an attempt failed (same wording as the migrate log)."""
    if report.get("passed"):
        return "passed (legacy output)" if report.get("golden_checked") else "passed (contract only)"
    first = next((v for t in report["tables"] for v in t["contract_violations"]), None)
    if first:
        lines = [x.strip() for x in first.splitlines() if x.strip()]
        errs = [x for x in lines if re.search(r"(Error|Exception)\b", x)]
        return (errs[0] if errs else lines[-1])[:300]
    return f"{sum(t['mismatched_keys'] for t in report['tables'])} rows differ from legacy output"


def collect(run_dir: Path, migrated: Path = ROOT / "migrated") -> dict:
    from factory.agents.discovery import load_jobs
    legacy = {j.job_id: j for j in load_jobs()}
    # summary.json is written when a run ends; the per-job files are written as each job settles.
    # Reading both (per-job wins) gives a dashboard for a run that is still going or was stopped:
    # jobs with no record yet are shown as "pending".
    recs = {}
    if (run_dir / "migrate" / "summary.json").exists():
        recs = {r["job_id"]: r for r in json.loads((run_dir / "migrate" / "summary.json").read_text())["jobs"]}
    for f in (run_dir / "migrate").glob("J*.json"):
        recs[f.stem] = json.loads(f.read_text())["record"]
    discs = {f.stem: json.loads(f.read_text()) for f in (run_dir / "discovery").glob("J*.json")}
    jobs = []
    for jid in sorted(discs, key=lambda k: (discs[k].get("wave") or 0, k)):
        disc = discs[jid]
        rec = recs.get(jid) or {"status": "pending", "language": legacy[jid].language if jid in legacy else "",
                                "wave": disc.get("wave"), "layer": None, "attempts": 0, "agent_seconds": 0}
        des = json.loads((run_dir / "design" / f"{jid}.json").read_text())
        mig_file = run_dir / "migrate" / f"{jid}.json"
        mig = json.loads(mig_file.read_text()) if mig_file.exists() else {"reports": [], "notes": []}
        code = migrated / jid / "job.py"
        if rec["status"] == "pending":           # a file left by a stopped attempt was never checked
            code = migrated / jid / "_not_run"
        tables = mig["reports"][-1]["tables"] if mig["reports"] else []
        jobs.append({
            "id": jid, "llm": mig.get("llm") or "", "status": rec["status"], "language": rec["language"], "wave": rec["wave"],
            "layer": rec["layer"], "attempts": rec["attempts"], "seconds": rec["agent_seconds"],
            "targets": des["spec"]["target_tables"], "depends_on": disc.get("depends_on", []),
            "summary": disc.get("summary") or "",
            "legacy_path": legacy[jid].path if jid in legacy else "",
            "legacy_code": legacy[jid].code if jid in legacy else "",
            "pyspark_path": str(code.relative_to(ROOT)) if code.exists() else "",
            "pyspark": code.read_text() if code.exists() else "",
            "tries": [{"attempt": r["attempt"], "passed": r["passed"], "why": _why(r),
                       "seconds": r["runtime_seconds"]} for r in mig["reports"]],
            "rows": [{"table": t["table"], "legacy": t["legacy_rows"], "migrated": t["migrated_rows"],
                      "mismatched": t["mismatched_keys"]} for t in tables],
            "risks": disc.get("risk_notes", []),
            "guardrails": des.get("notes", []),
            "translation": [n for n in mig.get("notes", []) if "validation failure" not in n],
        })
    meta = {}
    reports = [json.loads(f.read_text()) for f in (run_dir / "migrate").glob("J*.json")]
    engines = {r["engine"] for m in reports for r in m.get("reports", [])}
    meta["engine"] = ", ".join(sorted(engines)) or "-"
    models = sorted({j["llm"] for j in jobs if j["llm"]})
    meta["llm"] = ", ".join(models) or _llm_of(run_dir)
    return {"run": run_dir.name, "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "meta": meta, "jobs": jobs}


def _llm_of(run_dir: Path) -> str:
    """Provider/model of the translation agent (from the environment the dashboard is built in)."""
    import os
    p = os.getenv("LLM_PROVIDER", "")
    if p == "ollama":
        return f"ollama / {os.getenv('OLLAMA_MODEL', 'qwen2.5-coder:14b')}"
    if p == "gemini":
        return f"gemini / {os.getenv('GEMINI_MODEL', 'gemini-2.5-flash')}"
    return p or "-"


def render(data: dict) -> str:
    payload = json.dumps(data).replace("</", "<\\/")
    return TEMPLATE.replace("__DATA__", payload).replace("__RUN__", html.escape(data["run"]))


def latest_run(runs: Path = ROOT / "runs") -> Optional[Path]:
    """The run whose migrate results changed most recently (None when no run has any)."""
    best, when = None, 0.0
    for d in (runs.iterdir() if runs.is_dir() else []):
        if not (d / "discovery").is_dir():
            continue
        t = max((f.stat().st_mtime for f in (d / "migrate").glob("J*.json")), default=0.0)
        if t > when:
            best, when = d, t
    return best


def build(run_dir: Path) -> Path:
    """Write runs/<run_id>/dashboard.html and return its path."""
    out = run_dir / "dashboard.html"
    out.write_text(render(collect(run_dir)))
    return out


def show(page: Path) -> bool:
    """Open the page in the default browser. False when there is none (a server, a CI job)."""
    try:
        return bool(webbrowser.open(page.resolve().as_uri()))
    except Exception:
        return False


# The page is written without <html>/<head> so it can also be embedded; a browser needs these.
HEAD = ('<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">')


def make_server(run_dir: Optional[Path] = None, port: int = 8765) -> ThreadingHTTPServer:
    """Live dashboard on this machine only. Each page load re-reads the run (the latest one when
    none is named), so refreshing the browser follows a migration while it runs."""
    class Page(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path.split("?")[0] != "/":
                return self.send_error(404)
            run = run_dir or latest_run()
            try:
                if run is None:
                    raise FileNotFoundError("no run with migrate results under runs/ yet")
                code, body = 200, HEAD + render(collect(run))
            except Exception as e:                    # e.g. a job file read while it is being written
                code = 503
                body = (HEAD + "<title>Migration Factory Review</title><p style='font:16px system-ui;padding:24px'>"
                        f"The dashboard could not be built: {html.escape(type(e).__name__ + ': ' + str(e))}. "
                        "Refresh the page to try again.</p>")
            data = body.encode()
            self.send_response(code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args) -> None:        # keep the terminal quiet
            pass

    return ThreadingHTTPServer(("127.0.0.1", port), Page)


def serve(run_dir: Optional[Path] = None, port: int = 8765, open_browser: bool = True) -> None:
    try:
        server = make_server(run_dir, port)
    except OSError as e:
        sys.exit(f"port {port} is in use ({e.strerror}); pass --port=<another>")
    url = f"http://localhost:{port}/"
    shown = run_dir or latest_run()
    print(f"dashboard: {url}  ({'run ' + shown.name if shown else 'no run yet'}"
          f"{'' if run_dir else ', follows the latest run'})  -  Ctrl+C to stop", flush=True)
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print()
    finally:
        server.server_close()


def main() -> None:
    from factory.config import load_env
    load_env()
    flags = {a for a in sys.argv[1:] if a.startswith("--")}
    named = [a for a in sys.argv[1:] if not a.startswith("--")]
    run_dir = ROOT / named[0] if named else None
    if run_dir is not None and not (run_dir / "discovery").is_dir():
        sys.exit(f"{named[0]} is not a run folder (no discovery/ inside)")
    if "--serve" in flags:
        port = int(next((f.split("=", 1)[1] for f in flags if f.startswith("--port=")), 8765))
        return serve(run_dir, port, open_browser="--no-open" not in flags)
    run_dir = run_dir or latest_run()
    if run_dir is None:
        sys.exit("no run with migrate results found under runs/")
    out = build(run_dir)
    opened = "--no-open" not in flags and show(out)
    print(f"dashboard: {out.relative_to(ROOT)}" + ("  (opened in the browser)" if opened else ""))


TEMPLATE = r"""<title>Migration Factory Review</title>
<meta name="claude:viewport-fit-sides" content="cover">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Sans+Condensed:wght@600&display=swap">
<style>
/* Layout: control-room ledger. Summary strip, wave lanes, job ledger, per-job dossier. */
:root {
  --bg: #f3f5f6; --surface: #ffffff; --sunk: #e8ecee; --line: #d5dce0;
  --fg: #16212b; --muted: #5a6873; --accent: #1f5f8b;
  --ok: #1d7a4c; --ok-bg: #dcf0e4; --fix: #1f5f8b; --fix-bg: #dce9f3;
  --warn: #9a5b00; --warn-bg: #f8e9cc; --stop: #6b7680; --stop-bg: #e3e7ea; --bad: #b03a2e;
  --display: "IBM Plex Sans Condensed", "Arial Narrow", system-ui, sans-serif;
  --body: "IBM Plex Sans", system-ui, -apple-system, sans-serif;
  --mono: "IBM Plex Mono", ui-monospace, Menlo, monospace;
}
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
  --bg: #0f151a; --surface: #161e25; --sunk: #1d272f; --line: #2a3640;
  --fg: #e3e9ed; --muted: #95a3ad; --accent: #6fb0dc;
  --ok: #5cc98d; --ok-bg: #173527; --fix: #6fb0dc; --fix-bg: #162c3c;
  --warn: #e7aa4c; --warn-bg: #3a2a10; --stop: #95a3ad; --stop-bg: #232d35; --bad: #ec7b6e; color-scheme: dark } }
:root[data-theme="dark"] {
  --bg: #0f151a; --surface: #161e25; --sunk: #1d272f; --line: #2a3640;
  --fg: #e3e9ed; --muted: #95a3ad; --accent: #6fb0dc;
  --ok: #5cc98d; --ok-bg: #173527; --fix: #6fb0dc; --fix-bg: #162c3c;
  --warn: #e7aa4c; --warn-bg: #3a2a10; --stop: #95a3ad; --stop-bg: #232d35; --bad: #ec7b6e; color-scheme: dark }
* { box-sizing: border-box }
body { margin: 0; background: var(--bg); color: var(--fg); font: 14px/1.5 var(--body);
  padding-left: max(16px, env(safe-area-inset-left, 0px)); padding-right: max(16px, env(safe-area-inset-right, 0px)); padding-block: 24px 48px }
.wrap { max-width: 1180px; margin: 0 auto; display: grid; gap: 28px }
h1, h2 { font-family: var(--display); font-weight: 600; margin: 0; text-wrap: balance; letter-spacing: .01em }
h1 { font-size: 30px; line-height: 1.15 } h2 { font-size: 19px }
.eyebrow { font: 500 11px/1 var(--mono); letter-spacing: .08em; text-transform: uppercase; color: var(--muted) }
.meta { display: flex; flex-wrap: wrap; gap: 6px 20px; color: var(--muted); font-size: 13px; margin-top: 8px }
.meta b { color: var(--fg); font-weight: 500 }
header { display: grid; gap: 6px }
.kpis { display: grid; grid-template-columns: 1.6fr repeat(4, 1fr); border: 1px solid var(--line); background: var(--surface); border-radius: 6px }
.kpi { padding: 16px 18px; display: grid; gap: 4px; border-left: 1px solid var(--line); min-width: 0 }
.kpi:first-child { border-left: 0 }
.kpi .n { font: 600 30px/1 var(--display); font-variant-numeric: tabular-nums }
.kpi .n small { font-size: 16px; color: var(--muted); font-weight: 500 }
.kpi .l { color: var(--muted); font-size: 13px }
.bar { display: flex; height: 8px; border-radius: 4px; overflow: hidden; background: var(--sunk); margin-top: 8px }
.bar span { display: block; height: 100% }
@media (max-width: 760px) { .kpis { grid-template-columns: 1fr 1fr } .kpi:first-child { grid-column: 1 / -1 }
  .kpi { border-left: 0; border-top: 1px solid var(--line) } .kpi:first-child { border-top: 0 } }
section { display: grid; gap: 12px }
.lanes { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px }
.lane { background: var(--sunk); border-radius: 6px; padding: 12px; display: grid; gap: 8px; align-content: start }
.lane h3 { margin: 0; font: 500 12px/1 var(--mono); color: var(--muted); letter-spacing: .06em; text-transform: uppercase }
.chip { display: grid; grid-template-columns: 8px 1fr auto; gap: 10px; align-items: center; background: var(--surface);
  border: 1px solid var(--line); border-radius: 4px; padding: 8px 10px; color: inherit; text-decoration: none; font-size: 13px }
.chip:hover, .chip:focus-visible { border-color: var(--accent); outline: none }
.chip .dot { width: 8px; height: 8px; border-radius: 50% }
.chip .dep { color: var(--muted); font: 11px var(--mono) }
.tbl { overflow-x: auto; border: 1px solid var(--line); border-radius: 6px; background: var(--surface) }
table { border-collapse: collapse; width: 100%; min-width: 820px; font-variant-numeric: tabular-nums }
th, td { text-align: left; padding: 9px 12px; border-bottom: 1px solid var(--line); vertical-align: middle }
th { font: 500 11px/1.2 var(--mono); letter-spacing: .06em; text-transform: uppercase; color: var(--muted); background: var(--sunk) }
tr:last-child td { border-bottom: 0 }
td.num { text-align: right } th.num { text-align: right }
td a { color: var(--accent); text-decoration: none; font-family: var(--mono); font-size: 13px }
td a:hover { text-decoration: underline }
.pill { display: inline-block; padding: 2px 9px; border-radius: 999px; font-size: 12px; font-weight: 500; white-space: nowrap }
.s-matched { color: var(--ok); background: var(--ok-bg) } .s-repaired { color: var(--fix); background: var(--fix-bg) }
.s-needs_review { color: var(--warn); background: var(--warn-bg) } .s-blocked { color: var(--stop); background: var(--stop-bg) }
.s-validated { color: var(--ok); background: var(--ok-bg) }
.s-pending { color: var(--muted); box-shadow: inset 0 0 0 1px var(--line) }
.tries { display: inline-flex; gap: 4px } .tries i { width: 10px; height: 10px; border-radius: 2px; display: inline-block }
.t-ok { background: var(--ok) } .t-bad { background: var(--bad) }
.lang { font: 500 11px var(--mono); color: var(--muted); text-transform: uppercase }
.diffwarn { color: var(--bad) }
details.job { background: var(--surface); border: 1px solid var(--line); border-radius: 6px }
details.job[open] { border-color: var(--accent) }
details.job > summary { cursor: pointer; list-style: none; padding: 12px 16px; display: flex; flex-wrap: wrap; gap: 8px 14px; align-items: center }
details.job > summary::-webkit-details-marker { display: none }
details.job > summary:focus-visible { outline: 2px solid var(--accent); outline-offset: -2px }
details.job > summary .id { font: 500 14px var(--mono) }
details.job > summary .arrow { margin-left: auto; color: var(--muted); font-size: 12px }
.dossier { border-top: 1px solid var(--line); padding: 16px; display: grid; gap: 16px }
.dossier p { margin: 0; max-width: 75ch; color: var(--muted) }
.code2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px }
@media (max-width: 860px) { .code2 { grid-template-columns: 1fr } }
.codebox { min-width: 0; display: grid; gap: 6px; align-content: start }
.codebox .hd { display: flex; justify-content: space-between; gap: 8px; flex-wrap: wrap }
.codebox .path { font: 12px var(--mono); color: var(--muted); overflow-wrap: anywhere }
pre { margin: 0; background: var(--sunk); border-radius: 4px; padding: 12px; overflow: auto; max-height: 460px;
  font: 12px/1.55 var(--mono); tab-size: 4 }
.draft { font-size: 12px; color: var(--warn) }
.cols3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px }
@media (max-width: 860px) { .cols3 { grid-template-columns: 1fr } }
.cols3 > div { min-width: 0 }
ul.notes { margin: 6px 0 0; padding-left: 18px; display: grid; gap: 4px; font-size: 13px }
ul.notes li { overflow-wrap: anywhere }
ol.att { margin: 6px 0 0; padding: 0; list-style: none; display: grid; gap: 6px }
ol.att li { display: grid; grid-template-columns: 80px 1fr auto; gap: 10px; font-size: 13px; align-items: start }
ol.att li code { font: 12px var(--mono); overflow-wrap: anywhere; min-width: 0 }
ol.att .n { font: 500 12px var(--mono); color: var(--muted) }
.legend { display: flex; flex-wrap: wrap; gap: 14px; font-size: 12px; color: var(--muted) }
.legend span { display: inline-flex; align-items: center; gap: 6px } .legend i { width: 10px; height: 10px; border-radius: 50%; display: inline-block }
footer { color: var(--muted); font-size: 12px }
@media (prefers-reduced-motion: no-preference) { details.job[open] .dossier { animation: in .18s ease-out } @keyframes in { from { opacity: .4 } to { opacity: 1 } } }
</style>
<div class="wrap">
  <header>
    <span class="eyebrow">Agentic Migration Factory &middot; review</span>
    <h1>Legacy T-SQL and SAS to PySpark: conversion results</h1>
    <div class="meta" id="meta"></div>
  </header>
  <div class="kpis" id="kpis"></div>
  <section>
    <h2>Run order by wave</h2>
    <div class="legend" id="legend"></div>
    <div class="lanes" id="lanes"></div>
  </section>
  <section>
    <h2>Job ledger</h2>
    <div class="tbl"><table><thead><tr>
      <th>Job</th><th>Source</th><th>Target table</th><th>Status</th><th>Attempts</th>
      <th class="num">Legacy rows</th><th class="num">Migrated rows</th><th class="num">Rows differing</th><th class="num">Agent time</th>
    </tr></thead><tbody id="rows"></tbody></table></div>
  </section>
  <section>
    <h2>Converted code and agent notes</h2>
    <div id="jobs" style="display:grid;gap:10px"></div>
  </section>
  <footer id="foot"></footer>
</div>
<script type="application/json" id="data">__DATA__</script>
<script>
(function () {
  const D = JSON.parse(document.getElementById("data").textContent);
  const LABEL = { matched: "Matched first try", repaired: "Repaired", needs_review: "Needs review",
                  blocked: "Blocked", validated: "Contract only", approved: "Approved", pending: "Not run yet" };
  const COLOR = { matched: "var(--ok)", repaired: "var(--fix)", needs_review: "var(--warn)", blocked: "var(--stop)", validated: "var(--ok)", pending: "var(--line)" };
  const PASS = new Set(["matched", "repaired", "validated", "approved"]);
  const el = (t, a = {}, ...kids) => { const e = document.createElement(t);
    for (const [k, v] of Object.entries(a)) k === "text" ? e.textContent = v : e.setAttribute(k, v);
    for (const k of kids) if (k != null) e.append(k); return e; };
  const pill = s => el("span", { class: "pill s-" + s, text: LABEL[s] || s });
  const fmtT = s => s >= 60 ? Math.round(s / 60) + " min" : s > 0 ? Math.round(s) + " s" : "-";
  const short = id => id.split("_")[0];
  const jobs = D.jobs, n = jobs.length;
  const by = s => jobs.filter(j => j.status === s).length;
  const passed = jobs.filter(j => PASS.has(j.status)).length;

  const meta = document.getElementById("meta");
  [["Run", D.run], ["Translation model", D.meta.llm], ["Engine", D.meta.engine], ["Generated", D.generated]]
    .forEach(([k, v]) => meta.append(el("span", {}, k + " ", el("b", { text: v }))));

  const k = document.getElementById("kpis");
  const big = el("div", { class: "kpi" },
    el("span", { class: "l", text: "Jobs reconciled with legacy output" }),
    el("span", { class: "n" }, String(passed), el("small", { text: " / " + n + "  (" + Math.round(100 * passed / n) + "%)" })));
  const bar = el("div", { class: "bar", role: "img", "aria-label": "Status of all jobs" });
  ["matched", "repaired", "validated", "needs_review", "blocked", "pending"].forEach(s => {
    const c = by(s); if (c) bar.append(el("span", { style: `width:${100 * c / n}%;background:${COLOR[s]}`, title: LABEL[s] + ": " + c })); });
  big.append(bar); k.append(big);
  [["matched", "Matched on the first try"], ["repaired", "Fixed by the repair loop"],
   ["needs_review", "Out of attempts: human review"], ["blocked", "Waiting on an upstream job"]]
    .concat(by("pending") ? [["pending", "Not run yet"]] : [])
    .forEach(([s, l]) => k.append(el("div", { class: "kpi" },
      el("span", { class: "l", text: l }), el("span", { class: "n", text: String(by(s)), style: s === "pending" ? "" : `color:${COLOR[s]}` }))));

  const lg = document.getElementById("legend");
  ["matched", "repaired", "needs_review", "blocked"].concat(by("pending") ? ["pending"] : []).forEach(s =>
    lg.append(el("span", {}, el("i", { style: "background:" + COLOR[s] }), LABEL[s])));
  const lanes = document.getElementById("lanes");
  [...new Set(jobs.map(j => j.wave))].sort().forEach(w => {
    const lane = el("div", { class: "lane" }, el("h3", { text: "Wave " + w }));
    jobs.filter(j => j.wave === w).forEach(j => lane.append(el("a", { class: "chip", href: "#" + j.id },
      el("span", { class: "dot", style: "background:" + COLOR[j.status] }),
      el("span", {}, el("span", { text: j.id }), el("br"), el("span", { class: "dep",
        text: j.depends_on.length ? "after " + j.depends_on.map(short).join(", ") : "reads bronze only" })),
      el("span", { class: "lang", text: j.language }))));
    lanes.append(lane);
  });

  const rows = document.getElementById("rows");
  jobs.forEach(j => {
    const tries = el("span", { class: "tries", "aria-label": j.tries.length + " attempts" });
    j.tries.forEach(t => tries.append(el("i", { class: t.passed ? "t-ok" : "t-bad", title: "Attempt " + t.attempt + ": " + t.why })));
    if (!j.tries.length) tries.append(el("span", { class: "lang", text: "not run" }));
    const r = j.rows[0] || {}, ran = j.rows.length && j.status !== "blocked";
    const diff = ran ? (r.mismatched || 0) : null;
    rows.append(el("tr", {},
      el("td", {}, el("a", { href: "#" + j.id, text: j.id })),
      el("td", {}, el("span", { class: "lang", text: j.language })),
      el("td", { text: j.targets.join(", ") }),
      el("td", {}, pill(j.status)), el("td", {}, tries),
      el("td", { class: "num", text: ran && r.legacy ? r.legacy.toLocaleString() : "-" }),
      el("td", { class: "num", text: ran && r.migrated ? r.migrated.toLocaleString() : "-" }),
      el("td", { class: "num" + (diff ? " diffwarn" : ""), text: diff == null ? "-" : diff.toLocaleString() }),
      el("td", { class: "num", text: fmtT(j.seconds) })));
  });

  const box = document.getElementById("jobs");
  const list = (title, items) => el("div", {}, el("span", { class: "eyebrow", text: title }),
    items.length ? (() => { const u = el("ul", { class: "notes" }); items.forEach(i => u.append(el("li", { text: i }))); return u; })()
                 : el("p", { text: "None." }));
  jobs.forEach(j => {
    const d = el("details", { class: "job", id: j.id });
    d.append(el("summary", {}, el("span", { class: "id", text: j.id }), pill(j.status),
      el("span", { class: "lang", text: j.language + " \u2192 " + j.targets.join(", ") }),
      j.llm ? el("span", { class: "lang", text: "written by " + j.llm.split("/").pop() }) : null,
      el("span", { class: "arrow", text: "Show" })));
    const body = el("div", { class: "dossier" });
    if (j.summary) body.append(el("p", { text: j.summary }));
    const ok = PASS.has(j.status);
    const conv = el("div", { class: "codebox" },
      el("div", { class: "hd" }, el("span", { class: "eyebrow", text: "Converted PySpark" }), el("span", { class: "path", text: j.pyspark_path || "not generated" })),
      !ok && j.pyspark ? el("span", { class: "draft", text: "Last draft: this code did not reconcile and needs review." }) : null,
      el("pre", {}, el("code", { text: j.pyspark || (j.status === "blocked" ? "Not generated: an upstream job did not pass." : j.status === "pending" ? "Not run yet." : "No code produced.") })));
    const leg = el("div", { class: "codebox" },
      el("div", { class: "hd" }, el("span", { class: "eyebrow", text: "Legacy " + j.language.toUpperCase() }), el("span", { class: "path", text: j.legacy_path })),
      el("pre", {}, el("code", { text: j.legacy_code })));
    body.append(el("div", { class: "code2" }, conv, leg));
    const att = el("div", {}, el("span", { class: "eyebrow", text: "Attempts" }));
    if (j.tries.length) { const o = el("ol", { class: "att" });
      j.tries.forEach(t => o.append(el("li", {}, el("span", { class: "n", text: "Attempt " + t.attempt }),
        el("code", { text: t.why, style: "color:" + (t.passed ? "var(--ok)" : "var(--fg)") }), el("span", { class: "lang", text: fmtT(t.seconds) }))));
      att.append(o);
    } else att.append(el("p", { text: j.status === "pending" ? "Not run yet." : "Not run: " + (j.depends_on.length ? "upstream " + j.depends_on.map(short).join(", ") + " did not pass." : "no attempts recorded.") }));
    body.append(att);
    body.append(el("div", { class: "cols3" }, list("Discovery: legacy risks", j.risks),
      list("Design: guardrails applied", j.guardrails), list("Translation: agent assumptions", j.translation)));
    d.append(body);
    d.addEventListener("toggle", () => d.querySelector(".arrow").textContent = d.open ? "Hide" : "Show");
    box.append(d);
  });
  const openHash = () => { const t = location.hash && document.getElementById(location.hash.slice(1));
    if (t && t.tagName === "DETAILS") { t.open = true; t.scrollIntoView({ block: "start" }); } };
  window.addEventListener("hashchange", openHash); openHash();
  document.getElementById("foot").textContent = "Each converted job is checked against its data contract and diffed row by row "
    + "against the legacy output (legacy/golden). Code lives in migrated/<job>/job.py; earlier attempts in migrated/<job>/attempts/.";
})();
</script>
"""

if __name__ == "__main__":
    sys.exit(main())
