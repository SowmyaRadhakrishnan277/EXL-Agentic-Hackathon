/* ---------------------------------------------------------------------------
   J06  Earned premium by policy and year      schedule: nightly 02:00
   Source : stg.policies
   Target : stg.policy_earned_premium
   Rule   : earned = annual premium x days in force in the year / 365
--------------------------------------------------------------------------- */
DROP TABLE IF EXISTS stg.policy_earned_premium;

SELECT
    p.policy_id,
    p.product_code,
    y.yr                                               AS policy_year,
    d.days_in_force,
    ROUND(p.annual_premium * d.days_in_force / 365.0, 2) AS earned_premium
INTO stg.policy_earned_premium
FROM stg.policies p
CROSS JOIN (VALUES (2021), (2022), (2023), (2024), (2025)) AS y(yr)
CROSS APPLY (
    SELECT DATEDIFF(DAY,
               IIF(p.start_date > DATEFROMPARTS(y.yr, 1, 1),  p.start_date, DATEFROMPARTS(y.yr, 1, 1)),
               IIF(p.end_date   < DATEFROMPARTS(y.yr, 12, 31), p.end_date,  DATEFROMPARTS(y.yr, 12, 31))
           ) + 1 AS days_in_force
) d
WHERE p.start_date <= DATEFROMPARTS(y.yr, 12, 31)
  AND p.end_date   >= DATEFROMPARTS(y.yr, 1, 1);
