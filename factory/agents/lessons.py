"""
Lessons memory: the mistakes the translation agent has already made, and what fixed them.

Without it every job starts cold: J09 repeats the NameError that J05 hit ten minutes earlier and
spends an attempt rediscovering it. With it, a failure is written down the moment it happens and
every later prompt (the next attempt, the next job, the other lane, the next run) is told about it:

    failed attempt  ->  failure(...)   "Error seen on J05: NameError: name 'upper' is not defined"
    next attempt passes -> fixed(...)  the same entry gains the cause and the fix, marked confirmed

Only fixes that were PROVEN (the attempt reconciled with the legacy output) are stored as fixes; a
diagnosis from an attempt that still failed may be wrong, so it is not kept. Errors are stored as
symptoms straight away, because knowing the mistake is useful even before it is fixed.

Kept in runs/lessons.json (LESSONS_PATH to change), shared by all runs and safe to use from the
two migration lanes at once.

    python -m factory.agents.lessons runs/<run_id>/migrate [more migrate folders]   # learn from past runs
    python -m factory.agents.lessons                                                # print the memory
"""
from __future__ import annotations

import json
import os
import re
import sys
import threading
from pathlib import Path
from typing import Optional

from factory.config import ROOT

MAX_IN_PROMPT = 8
MAX_UNCONFIRMED = 4


def _path() -> Path:
    return Path(os.getenv("LESSONS_PATH") or ROOT / "runs" / "lessons.json")


def _key(text: str) -> str:
    """Same mistake, different names -> same key: quoted names, table names and numbers removed."""
    s = re.sub(r"'[^']*'|\"[^\"]*\"|`[^`]*`", "<x>", text)
    s = re.sub(r"\b(silver|gold|bronze|stg|dbo|rpt|legacy)\.\w+", "<table>", s)
    s = re.sub(r"\d+", "<n>", s)
    return re.sub(r"\s+", " ", s).strip().lower()[:120]


def worth_keeping(symptom: str) -> bool:
    """An error message that tells the next attempt something (not a bare stack frame)."""
    s = symptom.strip()
    return (len(s) >= 25 and not s.startswith("at ") and "rows differ from legacy" not in s
            and not s.startswith("no usable code"))


class Lessons:
    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = path or _path()
        self._lock = threading.Lock()
        try:
            self.items: list[dict] = json.loads(self.path.read_text())
        except (OSError, ValueError):
            self.items = []

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.items, indent=1))

    def _find(self, key: str) -> Optional[dict]:
        return next((i for i in self.items if i["key"] == key), None)

    def failure(self, job_id: str, language: str, symptom: str) -> None:
        """An attempt failed with this error. Recorded at once, before anyone knows the fix."""
        if not worth_keeping(symptom):
            return
        with self._lock:
            it = self._find(_key(symptom))
            if it is None:
                it = {"key": _key(symptom), "symptom": symptom.strip()[:300], "fix": None,
                      "confirmed": False, "seen": 0, "jobs": [], "languages": []}
                self.items.append(it)
            it["seen"] += 1
            for field, v in (("jobs", job_id[:3]), ("languages", language)):
                if v not in it[field]:
                    it[field].append(v)
            self._save()

    def fixed(self, job_id: str, language: str, symptom: str, fix: str, model: str = "") -> None:
        """The attempt after `symptom` reconciled with legacy: `fix` (its diagnosis and change) is proven."""
        if not fix.strip():
            return
        if symptom.startswith("no usable code"):
            symptom = "the output did not reconcile with the legacy output"
        mismatch = not worth_keeping(symptom)          # row differences: specific to this job
        key = f"mismatch:{job_id[:3]}:{_key(fix)[:60]}" if mismatch else _key(symptom)
        with self._lock:
            it = self._find(key)
            if it is None:
                it = {"key": key, "symptom": symptom.strip()[:300], "fix": None, "confirmed": False,
                      "seen": 1, "jobs": [], "languages": []}
                self.items.append(it)
            it.update(fix=fix.strip()[:500], confirmed=True, fixed_by=model)
            for field, v in (("jobs", job_id[:3]), ("languages", language)):
                if v not in it[field]:
                    it[field].append(v)
            self._save()

    def for_prompt(self, job_id: str, language: str) -> str:
        """The lessons most likely to help this job, as prompt text ('' when there are none)."""
        with self._lock:
            items = list(self.items)
        me = job_id[:3]
        rank = lambda i: (me not in i["jobs"], not i["confirmed"], language not in i["languages"], -i["seen"])
        picked, loose = [], 0
        for it in sorted(items, key=rank):
            if not it["confirmed"]:
                if loose >= MAX_UNCONFIRMED:
                    continue
                loose += 1
            picked.append(it)
            if len(picked) >= MAX_IN_PROMPT:
                break
        lines = []
        for it in picked:
            where = "/".join(it["jobs"][:4])
            if it["confirmed"]:
                lines.append(f"- [{where}, fixed] Problem: {it['symptom']}\n  What fixed it: {it['fix']}")
            else:
                lines.append(f"- [{where}, seen {it['seen']}x, not yet fixed] Error: {it['symptom']}")
        return "\n".join(lines)


def learn_from_run(lessons: Lessons, migrate_dir: Path) -> int:
    """Replay the records of a finished run into the memory. Returns the number of jobs read."""
    from factory.agents.dashboard import _why
    n = 0
    for f in sorted(migrate_dir.glob("J*.json")):
        d = json.loads(f.read_text())
        reports, lang = d.get("reports", []), d["record"]["language"]
        if not reports:
            continue
        n += 1
        notes = d.get("notes", [])
        for i, r in enumerate(reports):
            if not r["passed"]:
                lessons.failure(d["record"]["job_id"], lang, _why(r))
            elif i > 0:
                tag = f"attempt {r['attempt']}: "
                fix = " ".join(x[len(tag):] for x in notes
                               if x.startswith(tag + "diagnosis:") or x.startswith(tag + "change:"))
                lessons.fixed(d["record"]["job_id"], lang, _why(reports[i - 1]), fix, d.get("llm") or "")
    return n


def main() -> None:
    lessons = Lessons()
    for arg in sys.argv[1:]:
        print(f"  {arg}: {learn_from_run(lessons, ROOT / arg)} jobs read")
    done = sum(i["confirmed"] for i in lessons.items)
    print(f"lessons: {len(lessons.items)} ({done} with a proven fix)  ->  {lessons.path}")
    for it in lessons.items:
        print(f"  [{'fixed' if it['confirmed'] else 'open '}] {'/'.join(it['jobs'])}  {it['symptom'][:110]}")


if __name__ == "__main__":
    sys.exit(main())
