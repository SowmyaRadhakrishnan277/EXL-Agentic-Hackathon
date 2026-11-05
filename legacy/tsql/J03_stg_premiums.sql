/* ---------------------------------------------------------------------------
   J03  Premium instalment staging             schedule: nightly 01:20
   Source : dbo.premiums   (billing system extract, known to contain repeats)
   Target : stg.premiums
--------------------------------------------------------------------------- */
DROP TABLE IF EXISTS stg.premiums;

SELECT DISTINCT
    pm.premium_id,
    pm.policy_id,
    pm.due_date,
    pm.paid_date,
    pm.amount,
    CASE WHEN pm.paid_date IS NULL THEN 'N' ELSE 'Y' END AS paid_flag
INTO stg.premiums
FROM dbo.premiums pm;
