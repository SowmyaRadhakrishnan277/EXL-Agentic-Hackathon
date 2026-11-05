/* ---------------------------------------------------------------------------
   J02  Policy staging                         schedule: nightly 01:10
   Source : dbo.policies, dbo.products
   Target : stg.policies
--------------------------------------------------------------------------- */
DROP TABLE IF EXISTS stg.policies;

SELECT
    p.policy_id,
    p.customer_id,
    p.product_code,
    pr.line_of_business,
    p.broker_id,
    p.channel,
    p.start_date,
    COALESCE(p.end_date, CAST('9999-12-31' AS DATE))      AS end_date,
    CASE WHEN p.end_date IS NULL THEN 'Y' ELSE 'N' END     AS rolling_flag,
    UPPER(LTRIM(RTRIM(p.status)))                          AS status,
    p.annual_premium,
    p.payment_frequency
INTO stg.policies
FROM dbo.policies p
JOIN dbo.products pr
  ON pr.product_code = p.product_code;
