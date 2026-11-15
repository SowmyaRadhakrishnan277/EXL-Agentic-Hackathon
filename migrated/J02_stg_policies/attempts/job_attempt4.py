from pyspark.sql import functions as F


def run(spark, tables):
    policies = tables['dbo.policies']
    products = tables['dbo.products']

    # SQL Server default collation is case-insensitive and ignores trailing spaces,
    # so build a join key that mirrors that without touching the output column.
    p = policies.withColumn('_pc_key', F.upper(F.rtrim(F.col('product_code'))))
    pr = products.select(
        F.upper(F.rtrim(F.col('product_code'))).alias('_pc_key'),
        F.col('line_of_business').alias('line_of_business'),
    )

    joined = p.join(pr, on='_pc_key', how='inner')

    # rolling_flag is derived from the ORIGINAL end_date (NULL -> 'Y'), before the default.
    joined = joined.withColumn(
        'rolling_flag',
        F.when(F.col('end_date').isNull(), F.lit('Y')).otherwise(F.lit('N')),
    )

    result = joined.select(
        F.col('policy_id').alias('policy_id'),
        F.col('customer_id').alias('customer_id'),
        F.col('product_code').alias('product_code'),
        F.col('line_of_business').alias('line_of_business'),
        F.col('broker_id').alias('broker_id'),
        F.col('channel').alias('channel'),
        F.col('start_date').alias('start_date'),
        F.coalesce(F.col('end_date'), F.to_date(F.lit('9999-12-31'))).alias('end_date'),
        F.col('rolling_flag').alias('rolling_flag'),
        F.upper(F.trim(F.col('status'))).alias('status'),
        F.col('annual_premium').alias('annual_premium'),
        F.col('payment_frequency').alias('payment_frequency'),
    )

    return {'silver.policies': result}
