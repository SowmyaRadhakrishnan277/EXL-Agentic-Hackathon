/* ---------------------------------------------------------------------------
   J08  Monthly indemnity paid by product      schedule: monthly, BD1 06:00
   Source : dbo.claim_payments, stg.claims, stg.policies
   Target : rpt.monthly_claims_paid
   Used by: Finance month-end pack
--------------------------------------------------------------------------- */
DROP TABLE IF EXISTS rpt.monthly_claims_paid;

SELECT
    p.product_code,
    DATEFROMPARTS(YEAR(cp.payment_date), MONTH(cp.payment_date), 1) AS payment_month,
    COUNT(DISTINCT cp.claim_id)                                      AS claims_paid,
    COUNT(*)                                                         AS payments,
    ROUND(SUM(cp.amount), 2)                                         AS indemnity_paid
INTO rpt.monthly_claims_paid
FROM (SELECT DISTINCT payment_id, claim_id, payment_date, payment_type, amount
      FROM dbo.claim_payments) cp
JOIN stg.claims   c ON c.claim_id  = cp.claim_id
JOIN stg.policies p ON p.policy_id = c.policy_id
WHERE cp.payment_type = 'INDEMNITY'
  AND cp.payment_date IS NOT NULL
GROUP BY
    p.product_code,
    DATEFROMPARTS(YEAR(cp.payment_date), MONTH(cp.payment_date), 1);
