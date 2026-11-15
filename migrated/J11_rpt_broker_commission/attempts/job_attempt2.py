from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

def run(spark, tables):
    brokers = tables['dbo.brokers']
    policies = tables['stg.policies']
    
    # Filter active brokers and non-cancelled policies
    filtered_brokers = brokers.filter(F.upper(F.trim(brokers.active_flag)) == 'Y')
    filtered_policies = policies.filter(F.upper(F.trim(policies.status)) != 'CANCELLED')
    
    # Join policies with brokers
    joined_df = filtered_policies.join(filtered_brokers, on='broker_id', how='inner')
    
    # Calculate written year
    joined_df = joined_df.withColumn('written_year', F.year(joined_df.start_date))
    
    # Filter policies by start_date year between 2021 and 2025
    joined_df = joined_df.filter(F.year(joined_df.start_date).between(2021, 2025))
    
    # Aggregate data
    result_df = joined_df.groupBy('broker_id', 'written_year')\
        .agg(
            F.count('*').alias('policies_written'),
            F.sum(joined_df.annual_premium).alias('written_premium'),
            F.round(F.sum(joined_df.annual_premium * joined_df.commission_rate), 2).alias('commission_due')
        )
    
    # Cast columns to match target data contract
    result_df = result_df.withColumn('policies_written', F.col('policies_written').cast('bigint'))\
        .withColumn('written_premium', F.col('written_premium').cast(DecimalType(38, 2)))\
        .withColumn('commission_due', F.col('commission_due').cast(DecimalType(38, 2)))
    
    return {'gold.broker_commission': result_df}