from pyspark.sql import functions as F


def run(spark, tables):
    policies = tables['stg.policies']
    brokers = tables['dbo.brokers']

    # Convert string columns to uppercase and trim trailing spaces
    policies = policies.withColumn('status', F.upper(F.trim(policies['status'])))
    brokers = brokers.withColumn('active_flag', F.upper(F.trim(brokers['active_flag'])))

    # Filter policies and brokers based on conditions
    policies = policies.filter((policies['status'] != 'CANCELLED') & (policies['status'].isNotNull()) & (F.year(policies['start_date']) >= 2021) & (F.year(policies['start_date']) <= 2025))
    brokers = brokers.filter(brokers['active_flag'] == 'Y')

    # Join policies and brokers
    joined_df = policies.join(brokers, policies['broker_id'] == brokers['broker_id'], 'inner')

    # Aggregate data
    result_df = joined_df.groupBy('broker_id', F.year('start_date').alias('written_year'))
    result_df = result_df.agg(
        F.count('*').alias('policies_written'),
        F.sum('annual_premium').alias('written_premium'),
        F.round(F.sum('annual_premium') * F.col('commission_rate'), 2).alias('commission_due')
    )

    # Cast columns to match target data contract
    result_df = result_df.withColumn('policies_written', F.col('policies_written').cast('bigint'))
    result_df = result_df.withColumn('written_premium', F.col('written_premium').cast('decimal(38,2)'))
    result_df = result_df.withColumn('commission_due', F.col('commission_due').cast('decimal(38,2)'))

    return {'gold.broker_commission': result_df}