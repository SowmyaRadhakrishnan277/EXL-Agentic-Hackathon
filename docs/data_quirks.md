# Planted data quirks

The generator (`python -m data.generator.generate`, seed 2026) makes about 2% of values messy on purpose.
Each quirk behaves differently in T-SQL, SAS and Spark, which is what the reconciliation agent must catch.
Exact counts are in `data/raw/_manifest.json`.

| Quirk | Where | Why it breaks a naive migration |
|---|---|---|
| NULL amounts | `premiums.amount`, `claim_payments.amount`, `claims.reserve_amount` | SAS treats missing numerics as lower than any number (`if amount < 100` is true); Spark and T-SQL treat NULL comparisons as unknown. SAS `sum()` ignores missing values, but `a + b` gives missing. |
| NULL dates | `claims.loss_date`, `customers.date_of_birth`, `policies.end_date` (rolling cover) | Age and duration calculations, `BETWEEN` filters and "active on date X" logic differ when dates are missing. |
| Half-cent amounts (x.xx5) | `premiums.amount`, `claim_payments.amount` | T-SQL `ROUND()` and SAS `round()` round half away from zero; Spark `round()` does too, but `bround()` and Python's `round()` use banker's rounding, and `float` casts drift. |
| Trailing / leading spaces, mixed case | `policies.status`, `claims.status`, `customers.county` | T-SQL `=` ignores trailing spaces and the default collation is case-insensitive, so `status = 'EXPIRED'` matches `'expired  '`. Spark is exact and case-sensitive, so a straight translation loses rows. |
| Duplicate rows | `premiums`, `claim_payments` (exact copies) | Legacy jobs that `SELECT DISTINCT` or SAS `proc sort nodupkey` remove them; a translation that drops the dedup step inflates totals. |

## Row counts (50,000 policies, 2021–2025)

products 8 · brokers 40 · customers 35,000 · policies 50,000 · see _manifest.json
