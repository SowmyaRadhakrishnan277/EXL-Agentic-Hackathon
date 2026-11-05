/* ---------------------------------------------------------------------------
   J05  Claims staging                         schedule: nightly 01:30
   Source : dbo.claims
   Target : stg.claims
   Note   : rejected claims are excluded. Status is passed through unchanged.
--------------------------------------------------------------------------- */
DECLARE @asof DATE = '2025-12-31';

DROP TABLE IF EXISTS stg.claims;

SELECT
    c.claim_id,
    c.policy_id,
    c.loss_date,
    c.reported_date,
    c.claim_type,
    c.status,
    c.reserve_amount,
    DATEDIFF(DAY, c.reported_date, @asof) AS claim_age_days
INTO stg.claims
FROM dbo.claims c
WHERE c.status <> 'REJECTED';
