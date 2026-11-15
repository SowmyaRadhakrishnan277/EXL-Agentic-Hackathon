from pyspark.sql import functions as F


def run(spark, tables):
    claims = tables['dbo.claims']
    asof_date = '2025-12-31'

    # T-SQL: status <> 'REJECTED' is case-insensitive and ignores TRAILING spaces only.
    # Leading spaces still matter, so ' Rejected' is NOT equal to 'REJECTED' and is kept.
    # NULL status yields UNKNOWN and is dropped by the filter, as in T-SQL.
    filtered = claims.filter(
        F.upper(F.rtrim(F.col('status'))) != F.lit('REJECTED')
    )

    # DATEDIFF(DAY, reported_date, @asof) -> datediff(end, start)
    result = filtered.withColumn(
        'claim_age_days',
        F.datediff(F.lit(asof_date).cast('date'), F.col('reported_date'))
    ).select(
        'claim_id',
        'policy_id',
        'loss_date',
        'reported_date',
        'claim_type',
        'status',
        'reserve_amount',
        'claim_age_days',
    )

    return {'silver.claims': result}
