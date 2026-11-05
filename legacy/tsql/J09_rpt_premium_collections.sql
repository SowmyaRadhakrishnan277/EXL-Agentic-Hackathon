/* ---------------------------------------------------------------------------
   J09  Premium due vs collected by month      schedule: monthly, BD1 06:15
   Source : stg.premiums
   Target : rpt.premium_collections
   Used by: Credit control / arrears review
--------------------------------------------------------------------------- */
DROP TABLE IF EXISTS rpt.premium_collections;

SELECT
    DATEFROMPARTS(YEAR(pm.due_date), MONTH(pm.due_date), 1)              AS due_month,
    COUNT(*)                                                             AS instalments_due,
    SUM(pm.amount)                                                       AS amount_due,
    SUM(CASE WHEN pm.paid_flag = 'Y' THEN pm.amount ELSE 0 END)          AS amount_collected,
    SUM(pm.amount)
      - SUM(CASE WHEN pm.paid_flag = 'Y' THEN pm.amount ELSE 0 END)      AS arrears,
    CAST(100.0 * SUM(CASE WHEN pm.paid_flag = 'Y' THEN pm.amount ELSE 0 END)
         / NULLIF(SUM(pm.amount), 0) AS DECIMAL(9, 2))                   AS collection_rate_pct
INTO rpt.premium_collections
FROM stg.premiums pm
GROUP BY DATEFROMPARTS(YEAR(pm.due_date), MONTH(pm.due_date), 1);
