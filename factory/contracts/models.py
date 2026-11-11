"""
Agent contract: the data passed between every stage of the migration factory.

Pipeline (one legacy job at a time, ordered by wave):

    LegacyJob ──Discover──▶ DiscoveryResult ──Design──▶ DesignSpec
              ──Generate──▶ GeneratedJob ──Reconcile──▶ ReconciliationReport
              ──(mismatch: Repair → Generate again, max N attempts)──
              ──Review──▶ ReviewDecision

Every stage reads the previous stage's output and writes its own as JSON under
runs/<run_id>/<job_id>/<stage>.json, so any stage can be re-run or inspected alone.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------- shared enums

class SourceLanguage(str, Enum):
    TSQL = "tsql"
    SAS = "sas"


class Layer(str, Enum):
    BRONZE = "bronze"   # raw copies of source tables
    SILVER = "silver"   # cleaned, conformed, joined
    GOLD = "gold"       # report-ready aggregates


class JobStatus(str, Enum):
    PENDING = "pending"
    DISCOVERED = "discovered"
    DESIGNED = "designed"
    GENERATED = "generated"
    VALIDATED = "validated"          # passes its data contract; no legacy output to compare yet
    BLOCKED = "blocked"              # an upstream job did not pass, so this one was not run
    MATCHED = "matched"              # reconciled first time
    REPAIRED = "repaired"            # reconciled after self-repair
    NEEDS_REVIEW = "needs_review"    # repair attempts exhausted
    APPROVED = "approved"            # human signed off
    REJECTED = "rejected"            # human rejected


# ---------------------------------------------------------------- 0. input

class LegacyJob(BaseModel):
    """One legacy script, as written by the team in legacy/tsql or legacy/sas."""
    job_id: str = Field(..., examples=["J03_monthly_claims"])
    language: SourceLanguage
    path: str                         # e.g. legacy/tsql/J03_monthly_claims.sql
    code: str
    description: Optional[str] = None  # human note; the agent must not rely on it
    golden_outputs: list[str] = []     # paths to legacy output files (CSV)


# ---------------------------------------------------------------- 1. discover

class ColumnLineage(BaseModel):
    target_table: str
    target_column: str
    source_columns: list[str]          # "table.column"
    transformation: str                # plain-English rule, e.g. "SUM(paid_amount) by month"


class DiscoveryResult(BaseModel):
    job_id: str
    input_tables: list[str]
    output_tables: list[str]
    column_lineage: list[ColumnLineage]
    depends_on: list[str] = []         # job_ids whose outputs this job reads
    wave: Optional[int] = None         # set by the wave planner after all jobs are discovered
    summary: str                       # what the job does, in one or two sentences
    risk_notes: list[str] = []         # e.g. "SAS missing values compare low"
    method: str = "parser+llm"         # how lineage was obtained (for traceability)


# ---------------------------------------------------------------- 2. design

class ColumnContract(BaseModel):
    name: str
    dtype: str                         # spark type, e.g. "decimal(18,2)", "date", "string"
    nullable: bool = True
    rule: Optional[str] = None         # e.g. ">= 0", "in ('OPEN','CLOSED')"


class DataContract(BaseModel):
    table: str
    primary_key: list[str]             # used for row-level reconciliation
    columns: list[ColumnContract]


class DesignSpec(BaseModel):
    job_id: str
    layer: Layer
    target_tables: list[str]           # e.g. "gold.monthly_claims"
    contracts: list[DataContract]


# ---------------------------------------------------------------- 3. generate

class GeneratedJob(BaseModel):
    job_id: str
    attempt: int                       # 1 = first translation, 2+ = after repair
    pyspark_path: str                  # migrated/<job_id>/job.py
    test_path: str                     # migrated/<job_id>/test_job.py
    translation_notes: list[str] = []  # assumptions the agent made
    llm_provider: str
    llm_model: str


# ---------------------------------------------------------------- 4. reconcile

class TableDiff(BaseModel):
    table: str
    legacy_rows: int
    migrated_rows: int
    column_total_diffs: dict[str, float] = {}   # column -> legacy_sum - migrated_sum
    mismatched_keys: int = 0
    sample_mismatches: list[dict] = []          # up to ~10 rows, legacy vs migrated
    contract_violations: list[str] = []


class ReconciliationReport(BaseModel):
    job_id: str
    attempt: int
    passed: bool
    tables: list[TableDiff]
    runtime_seconds: float
    engine: str                        # "local-pyspark" or "databricks-serverless"
    golden_checked: bool = False       # True when compared with the legacy output, not just the contract


# ---------------------------------------------------------------- 5. review

class ReviewDecision(BaseModel):
    job_id: str
    decision: JobStatus                # APPROVED or REJECTED
    reviewer: str
    comment: Optional[str] = None
    decided_at: datetime


# ---------------------------------------------------------------- run summary

class JobRecord(BaseModel):
    """One row per job on the dashboard."""
    job_id: str
    language: SourceLanguage
    wave: Optional[int] = None
    layer: Optional[Layer] = None
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    agent_seconds: float = 0.0
    manual_estimate_hours: Optional[float] = None
