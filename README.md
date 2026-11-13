# Agentic Migration Factory — EXL × UCD Data & AI Hackathon 2026 (Challenge 03)

AI agents that migrate legacy T-SQL and SAS jobs to PySpark on a lakehouse, map
column-level lineage, and **prove** each migrated job matches the legacy output
record for record before a human approves it.

## Pipeline

| Stage | What happens | Output |
|---|---|---|
| 1. Discover | Parse legacy code, map column lineage, order jobs into waves | `DiscoveryResult` |
| 2. Design | Assign bronze/silver/gold layer and a data contract per table | `DesignSpec` |
| 3. Generate | Translate to PySpark / Spark SQL plus unit tests | `GeneratedJob` |
| 4. Reconcile | Run legacy and migrated side by side; diff counts, totals, rows | `ReconciliationReport` |
| 4b. Repair | On mismatch, feed the diff back to Generate (max 3 attempts) | new `GeneratedJob` |
| 5. Review | Human approves or rejects on the dashboard | `ReviewDecision` |

Contract definitions: [`factory/contracts/models.py`](factory/contracts/models.py).

## Repo layout

```
legacy/tsql/      legacy T-SQL jobs (input to the factory)
legacy/sas/       legacy SAS programs
legacy/golden/    outputs of the legacy jobs, used as the source of truth
data/generator/   synthetic insurance data generator
data/raw/         generated source tables
factory/          the agent pipeline (contracts, agents, llm, runtime)
migrated/         PySpark jobs, tests and contracts produced by the agents
runs/             JSON logs of every stage, per run and per job
dashboard/        Streamlit dashboard
docker/           SQL Server container for running the legacy T-SQL
config/           settings
docs/             architecture diagram, Responsible AI note
```

## Stack

- Legacy: SQL Server 2022 (Docker) for T-SQL, SAS OnDemand for Academics for SAS
- Target: PySpark locally for development, Databricks Free Edition (serverless, via Databricks Connect) for real runs
- Agents: plain Python; LLM provider is pluggable (`LLM_PROVIDER`)
- Dashboard: Streamlit

## Data

Synthetic data only. No personal or client data is used.

## Running it

```bash
pip install -r requirements.txt
cp .env.example .env                      # add GEMINI_API_KEY
python -m data.generator.generate         # synthetic data -> data/raw/
python -m factory.agents.discovery        # Stage 1 -> runs/<run_id>/discovery/
python -m eval.score_discovery runs/<run_id>
python -m factory.agents.design runs/<run_id>    # Stage 2 -> runs/<run_id>/design/
python -m factory.mapping_doc runs/<run_id>      # -> docs/source_to_target_mapping.md + .xlsx
python -m factory.bronze                         # raw CSVs -> bronze layer (needs pyspark + Java)
python -m factory.agents.migrate runs/<run_id>   # Stages 3-4: LLM writes, runs, checks and fixes
                                                 # each job; passing output -> silver / gold
pytest -q
```

Legacy golden outputs (needs Docker + SAS OnDemand): see `docs/legacy_jobs_answer_key.md`.
`eval/answer_key.yaml` is ground truth for scoring only and is never shown to an agent.

## Migration agent

`factory/agents/migrate.py` builds silver and gold job by job, in wave order:
the translation agent (LLM) writes the PySpark code, the code is safety-checked
(no file, network, process or eval access) and run on the lakehouse inputs, and the
output is checked against its data contract and, when `legacy/golden/` has it, the
legacy output (row counts, column totals, row-level diff on the primary key).
Any crash or difference goes back to the LLM to fix, up to 3 attempts; then the job is
flagged for review and its downstream jobs are blocked. Every attempt's code and
report is kept under `migrated/<job>/attempts/` and `runs/<run_id>/migrate/`.

## Reference translations

`reference_jobs/jobs.py` holds hand-written PySpark translations of all 12 legacy jobs, in the
same `run(spark, tables)` shape as the agents' output. They reproduce each planted trap the way
the legacy system behaves (pinned by `tests/test_reference_jobs.py`), give the demo a known-good
silver/gold build, and serve as a baseline to compare the agents' code against.

```bash
python -m reference_jobs.run                    # build silver + gold from bronze
python -m reference_jobs.run runs/<run_id>      # + check every table against legacy/golden/
```

On Databricks: import `databricks/01_setup_bronze.py` and `databricks/02_run_silver_gold.py`
(generated from the module by `databricks/build_notebook.py`), or create the Job in
`databricks/migration_job.json`: one task per legacy job, wired by the discovered lineage.

## Lakehouse layers

| Layer | Holds | Legacy source |
|---|---|---|
| bronze | raw extracts, typed from the legacy DDL, plus `_ingested_at`, `_source_file` | `dbo.*` |
| silver | cleaned / conformed / intermediate tables | `stg.*`, SAS staging |
| gold | report-ready tables | `rpt.*`, SAS reports |

`factory/catalog.py` is the single legacy-to-lakehouse name mapping; full table and column
mapping is in [`docs/source_to_target_mapping.md`](docs/source_to_target_mapping.md) and
`docs/source_to_target_mapping.xlsx` (Overview, Table / Job / Column Mapping, and a Migration
Status sheet once the migration agent has run). The design and migration agents regenerate the
workbook automatically at the end of each run.
Locally tables are parquet under `lakehouse/`; with `SPARK_ENGINE=databricks` they are Delta
tables `workspace.<layer>.<table>` in Unity Catalog. For Databricks, create a volume
`workspace.bronze.raw`, upload `data/raw/*.csv` to it, and set `RAW_PATH=/Volumes/workspace/bronze/raw`.
