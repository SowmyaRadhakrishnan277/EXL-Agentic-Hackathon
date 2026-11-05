/* ---------------------------------------------------------------------------
   J12  Open claim reserves by age band        schedule: monthly, BD2 06:00
   Source : stg.claims, stg.policies
   Target : rpt.open_reserves
   Used by: Reserving committee
--------------------------------------------------------------------------- */
DROP TABLE IF EXISTS rpt.open_reserves;

SELECT
    p.product_code,
    CASE
        WHEN c.claim_age_days <= 30  THEN '0-30'
        WHEN c.claim_age_days <= 90  THEN '31-90'
        WHEN c.claim_age_days <= 180 THEN '91-180'
        ELSE '180+'
    END                         AS age_band,
    COUNT(*)                    AS open_claims,
    COUNT(c.reserve_amount)     AS claims_with_reserve,
    SUM(c.reserve_amount)       AS total_reserve
INTO rpt.open_reserves
FROM stg.claims c
JOIN stg.policies p
  ON p.policy_id = c.policy_id
WHERE c.status = 'OPEN'
GROUP BY
    p.product_code,
    CASE
        WHEN c.claim_age_days <= 30  THEN '0-30'
        WHEN c.claim_age_days <= 90  THEN '31-90'
        WHEN c.claim_age_days <= 180 THEN '91-180'
        ELSE '180+'
    END;
