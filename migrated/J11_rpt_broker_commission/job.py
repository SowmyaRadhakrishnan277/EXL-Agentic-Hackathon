from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def run(spark, tables):
    brokers = tables['dbo.brokers']
    policies = tables['stg.policies']

    # Disambiguate the shared broker_id column before joining.
    b = brokers.select(
        F.col('broker_id').alias('broker_id'),
        F.col('commission_rate').cast(DecimalType(10, 6)).alias('commission_rate'),
        F.col('active_flag').alias('active_flag'),
    )
    p = policies.select(
        F.col('broker_id').alias('broker_id'),
        F.col('start_date').alias('start_date'),
        F.col('annual_premium').cast(DecimalType(20, 2)).alias('annual_premium'),
        F.col('status').alias('status'),
    )

    # T-SQL string comparison: case-insensitive, trailing spaces ignored,
    # leading spaces significant; NULL comparisons are unknown -> row dropped.
    b = b.filter(F.upper(F.rtrim(F.col('active_flag'))) == F.lit('Y'))
    p = p.filter(F.upper(F.rtrim(F.col('status'))) != F.lit('CANCELLED'))

    joined = p.join(b, on='broker_id', how='inner')

    # YEAR(p.start_date) BETWEEN 2021 AND 2025 (NULL start_date -> excluded)
    joined = joined.withColumn('written_year', F.year(F.col('start_date')))
    joined = joined.filter(F.col('written_year').between(2021, 2025))

    # Legacy multiplies per row, then sums: SUM(p.annual_premium * b.commission_rate)
    joined = joined.withColumn(
        'commission_amt',
        F.col('annual_premium') * F.col('commission_rate'),
    )

    result = joined.groupBy('broker_id', 'written_year').agg(
        F.count(F.lit(1)).alias('policies_written'),
        F.sum(F.col('annual_premium')).alias('written_premium'),
        F.round(F.sum(F.col('commission_amt')), 2).alias('commission_due'),
    )

    result = result.select(
        F.col('broker_id').cast('string').alias('broker_id'),
        F.col('written_year').cast('int').alias('written_year'),
        F.col('policies_written').cast('bigint').alias('policies_written'),
        F.col('written_premium').cast(DecimalType(38, 2)).alias('written_premium'),
        F.col('commission_due').cast(DecimalType(38, 2)).alias('commission_due'),
    )

    return {'gold.broker_commission': result}
