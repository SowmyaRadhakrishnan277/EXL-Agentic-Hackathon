# Legacy jobs: answer key

**Not an input to the agents.** This is the ground truth used to score the discovery agent
(lineage, dependencies, waves) and to check that each planted trap is caught. Keep it out
of any prompt.

## The 12 jobs

| Job | Lang | Output | Reads | Layer (expected) | Wave |
|---|---|---|---|---|---|
| J01 | T-SQL | stg.customers | dbo.customers | silver | 1 |
| J02 | T-SQL | stg.policies | dbo.policies, dbo.products | silver | 1 |
| J03 | T-SQL | stg.premiums | dbo.premiums | silver | 1 |
| J04 | SAS | stg_claim_payments | claim_payments.csv | silver | 1 |
| J05 | T-SQL | stg.claims | dbo.claims | silver | 1 |
| J06 | T-SQL | stg.policy_earned_premium | J02 | silver | 2 |
| J07 | SAS | claim_totals | J04, J05 | silver | 2 |
| J08 | T-SQL | rpt.monthly_claims_paid | dbo.claim_payments, J05, J02 | gold | 2 |
| J09 | T-SQL | rpt.premium_collections | J03 | gold | 2 |
| J11 | T-SQL | rpt.broker_commission | J02, dbo.brokers | gold | 2 |
| J12 | T-SQL | rpt.open_reserves | J05, J02 | gold | 2 |
| J10 | SAS | rpt_loss_ratio | J07, J02, J06 | gold | 3 |

## Planted traps: what a straight translation gets wrong

| Job | Trap | Legacy behaviour | Naive PySpark result |
|---|---|---|---|
| J01 | `DATEDIFF(YEAR, dob, @asof)` | Counts year boundaries, not true age (born 1990-12-01 → 35 at 2025-06-30) | `floor(months_between/12)` gives 34 for anyone whose birthday falls after 30 Jun |
| J02 | NULL `end_date` → 9999-12-31 | Rolling policies get an open end date | Dropping the COALESCE leaves NULLs that break J06 |
| J03 | `SELECT DISTINCT` | Exact duplicate instalments removed | Missing `.distinct()` inflates J09 totals |
| J04 | `nodupkey` + `amount < 100` | Missing amount < 100 is TRUE in SAS → band 'SMALL' | Spark NULL < 100 is NULL → falls to 'LARGE' |
| J05 | `status <> 'REJECTED'` | SQL Server ignores trailing spaces and case: 'rejected', 'REJECTED  ' are excluded; ' Rejected' (leading space) is kept | Spark compares exactly, so all messy variants are kept |
| J06 | `ROUND(decimal * int / 365.0, 2)` | Decimal arithmetic, half away from zero | Float maths or `bround` drifts by 0.01 on some rows |
| J07 | `indemnity + expense` | Missing if either is missing (claims with no expense rows → total missing → flag 'Y') | `coalesce(..., 0)` or Spark `+` semantics differ; flag logic changes |
| J08 | Inline DISTINCT on payments | Duplicates removed before summing | Missing dedup overstates paid |
| J09 | `COUNT(*)` vs NULL amounts | Instalments with NULL amount are counted but add nothing | Filtering NULLs changes `instalments_due` |
| J10 | NULL `loss_date` | `year(.)` missing → its own group in the FULL JOIN | Rows dropped or grouped differently |
| J11 | `active_flag = 'Y'`, `status <> 'CANCELLED'` | Clean after J02's trim/upper | Mostly safe: a control job that should match first time |
| J12 | `status = 'OPEN'` on untrimmed status | Matches 'open', 'OPEN  ' (case/trailing-insensitive), not ' Open' | Exact match finds fewer open claims |

## How to produce the golden outputs

1. `python -m data.generator.generate` → `python -m data.load_mssql` → `python -m legacy.run_legacy_tsql`
2. Upload `legacy/golden/sas_input/*.csv` to `~/exl/input/` in SAS OnDemand, create `~/exl/lib` and `~/exl/golden`
3. Run J04 → J07 → J10 in SAS Studio, then download `~/exl/golden/*.csv` into `legacy/golden/`
