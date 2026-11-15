from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, StructField, StructType

YEARS = [2021, 2022, 2023, 2024, 2025]


def run(spark, tables):
    policies = tables['stg.policies']

    # Inline VALUES (2021)..(2025) AS y(yr) -- hardcoded year range kept as in legacy
    years = spark.createDataFrame(
        [(y,) for y in YEARS],
        StructType([StructField('yr', IntegerType(), False)]),
    )

    p = policies.select(
        F.col('policy_id'),
        F.col('product_code'),
        F.col('annual_premium'),
        F.to_date(F.col('start_date')).alias('start_date'),
        F.to_date(F.col('end_date')).alias('end_date'),
    )

    # CROSS JOIN y(yr)
    j = p.crossJoin(years)

    jan1 = F.make_date(F.col('yr'), F.lit(1), F.lit(1))
    dec31 = F.make_date(F.col('yr'), F.lit(12), F.lit(31))

    # IIF(p.start_date > Jan1, p.start_date, Jan1)  -- NULL condition -> ELSE branch
    lo = F.when(F.col('start_date') > jan1, F.col('start_date')).otherwise(jan1)
    # IIF(p.end_date < Dec31, p.end_date, Dec31)    -- NULL condition -> ELSE branch
    hi = F.when(F.col('end_date') < dec31, F.col('end_date')).otherwise(dec31)

    # DATEDIFF(DAY, lo, hi) + 1  -> inclusive days in force
    j = j.withColumn(
        'days_in_force',
        (F.datediff(hi, lo) + F.lit(1)).cast(IntegerType()),
    )

    # WHERE p.start_date <= Dec31 AND p.end_date >= Jan1  (NULL -> unknown -> dropped)
    j = j.filter((F.col('start_date') <= dec31) & (F.col('end_date') >= jan1))

    # ROUND(p.annual_premium * d.days_in_force / 365.0, 2) -- decimal math, half away from zero
    j = j.withColumn(
        'earned_premium',
        F.round(
            F.col('annual_premium').cast(DecimalType(38, 6))
            * F.col('days_in_force').cast(DecimalType(38, 0))
            / F.lit(Decimal('365.0')),
            2,
        ).cast(DecimalType(38, 2)),
    )

    out = j.select(
        F.col('policy_id').cast('string').alias('policy_id'),
        F.col('product_code').cast('string').alias('product_code'),
        F.col('yr').cast(IntegerType()).alias('policy_year'),
        F.col('days_in_force').cast(IntegerType()).alias('days_in_force'),
        F.col('earned_premium').cast(DecimalType(38, 2)).alias('earned_premium'),
    )

    return {'silver.policy_earned_premium': out}
