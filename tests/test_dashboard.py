"""Dashboard: latest run, a run that is still going, and the live page. No Spark, no LLM."""
import json
import os
import shutil
import threading
import urllib.error
import urllib.request

import pytest

from factory.agents import dashboard
from factory.config import ROOT

RUN = ROOT / "runs" / "20261114-lanes"          # committed results of a full run
pytestmark = pytest.mark.skipif(not RUN.is_dir(), reason="the committed run is not in this checkout")


def part_run(tmp_path, name, done=("J03_stg_premiums",), todo=("J10_rpt_loss_ratio",)):
    """A run folder with discovery and design for some jobs, and a migrate result only for `done`."""
    run = tmp_path / name
    for sub in ("discovery", "design", "migrate"):
        (run / sub).mkdir(parents=True)
    for jid in (*done, *todo):
        for sub in ("discovery", "design"):
            shutil.copy(RUN / sub / f"{jid}.json", run / sub / f"{jid}.json")
    for jid in done:
        shutil.copy(RUN / "migrate" / f"{jid}.json", run / "migrate" / f"{jid}.json")
    return run


def test_run_without_a_summary_shows_finished_jobs_and_marks_the_rest_not_run(tmp_path):
    data = dashboard.collect(part_run(tmp_path, "r1"))
    by_id = {j["id"]: j for j in data["jobs"]}
    assert by_id["J03_stg_premiums"]["status"] == "matched" and by_id["J03_stg_premiums"]["pyspark"]
    assert by_id["J10_rpt_loss_ratio"]["status"] == "pending"
    assert by_id["J10_rpt_loss_ratio"]["pyspark"] == ""          # an unchecked draft is not shown as converted
    assert "Not run yet" in dashboard.render(data)


def test_latest_run_is_the_one_whose_results_changed_last(tmp_path):
    old, new = part_run(tmp_path, "old"), part_run(tmp_path, "new")
    (tmp_path / "llm_cache").mkdir()                              # not a run: no discovery/ inside
    os.utime(old / "migrate" / "J03_stg_premiums.json", (1_000, 1_000))
    assert dashboard.latest_run(tmp_path) == new
    os.utime(old / "migrate" / "J03_stg_premiums.json", None)     # a job of the old run settles now
    os.utime(new / "migrate" / "J03_stg_premiums.json", (1_000, 1_000))
    assert dashboard.latest_run(tmp_path) == old
    assert dashboard.latest_run(tmp_path / "missing") is None


def test_build_writes_the_page_into_the_run_folder(tmp_path):
    run = part_run(tmp_path, "r2")
    page = dashboard.build(run)
    assert page == run / "dashboard.html" and page.read_text().isascii()   # opens right without a charset


def test_live_page_rereads_the_run_on_every_load(tmp_path):
    run = part_run(tmp_path, "r3")
    server = dashboard.make_server(run, port=0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        def data():
            page = urllib.request.urlopen(url + "/", timeout=10).read().decode()
            assert page.startswith("<!doctype html>")
            return json.loads(page.split('id="data">')[1].split("</script>")[0].replace("<\\/", "</"))
        assert {j["id"][:3]: j["status"] for j in data()["jobs"]} == {"J03": "matched", "J10": "pending"}
        shutil.copy(RUN / "migrate" / "J10_rpt_loss_ratio.json", run / "migrate" / "J10_rpt_loss_ratio.json")
        assert {j["id"][:3]: j["status"] for j in data()["jobs"]} == {"J03": "matched", "J10": "repaired"}
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(url + "/etc/passwd", timeout=10)
        assert e.value.code == 404
    finally:
        server.shutdown()
        server.server_close()
