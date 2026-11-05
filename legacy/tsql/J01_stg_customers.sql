/* ---------------------------------------------------------------------------
   J01  Customer staging                       schedule: nightly 01:00
   Source : dbo.customers
   Target : stg.customers
   Note   : names are dropped here; downstream reporting does not need them.
--------------------------------------------------------------------------- */
DECLARE @asof DATE = '2025-06-30';   -- age reporting date used by actuarial

DROP TABLE IF EXISTS stg.customers;

SELECT
    c.customer_id,
    UPPER(LTRIM(RTRIM(c.county)))            AS county,
    c.date_of_birth,
    DATEDIFF(YEAR, c.date_of_birth, @asof)   AS age_years,
    c.customer_since,
    c.segment
INTO stg.customers
FROM dbo.customers c;
