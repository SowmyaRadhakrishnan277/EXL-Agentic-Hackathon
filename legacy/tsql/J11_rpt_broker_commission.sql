/* ---------------------------------------------------------------------------
   J11  Broker commission by year written      schedule: monthly, BD3 07:00
   Source : stg.policies, dbo.brokers
   Target : rpt.broker_commission
   Rule   : commission on written premium, active brokers only,
            cancelled policies excluded
--------------------------------------------------------------------------- */
DROP TABLE IF EXISTS rpt.broker_commission;

SELECT
    b.broker_id,
    YEAR(p.start_date)                                AS written_year,
    COUNT(*)                                          AS policies_written,
    SUM(p.annual_premium)                             AS written_premium,
    ROUND(SUM(p.annual_premium * b.commission_rate), 2) AS commission_due
INTO rpt.broker_commission
FROM stg.policies p
JOIN dbo.brokers b
  ON b.broker_id = p.broker_id
WHERE b.active_flag = 'Y'
  AND p.status <> 'CANCELLED'
  AND YEAR(p.start_date) BETWEEN 2021 AND 2025
GROUP BY b.broker_id, YEAR(p.start_date);
