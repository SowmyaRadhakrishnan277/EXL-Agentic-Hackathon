from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType

def run(spark, tables):
    policies = tables['stg.policies']
    years = spark.createDataFrame([(2021,), (2022,), (2023,), (2024,), (2025,)], ['yr'])
    
    # Calculate days in force
    policies = policies.withColumn('start_date', F.to_date('start_date'))
    policies = policies.withColumn('end_date', F.to_date('end_date'))
    
    days_in_force = F.when(
        (F.col('start_date') > F.make_date(F.col('yr'), 1, 1)) & (F.col('end_date') < F.make_date(F.col('yr'), 12, 31)),
        F.datediff(F.col('end_date'), F.col('start_date')) + 1
    ).when(
        F.col('start_date') > F.make_date(F.col('yr'), 1, 1),
        F.datediff(F.make_date(F.col('yr'), 12, 31), F.col('start_date')) + 1
    ).when(
        F.col('end_date') < F.make_date(F.col('yr'), 12, 31),
        F.datediff(F.col('end_date'), F.make_date(F.col('yr'), 1, 1)) + 1
    ).otherwise(
        F.datediff(F.make_date(F.col('yr'), 12, 31), F.make_date(F.col('yr'), 1, 1)) + 1
    )
    
    policy_earned_premium = policies.crossJoin(years)
    policy_earned_premium = policy_earned_premium.withColumn('days_in_force', days_in_force)
    policy_earned_premium = policy_earned_premium.withColumn('earned_premium', F.round(F.col('annual_premium') * F.col('days_in_force') / 365.0, 2))
    
    policy_earned_premium = policy_earned_premium.filter(
        (F.col('start_date') <= F.make_date(F.col('yr'), 12, 31)) &
        (F.col('end_date') >= F.make_date(F.col('yr'), 1, 1))
    )
    
    policy_earned_premium = policy_earned_premium.select(
        'policy_id',
        'product_code',
        F.col('yr').alias('policy_year'),
        'days_in_force',
        'earned_premium'
    )
    
    policy_earned_premium = policy_earned_premium.withColumn('policy_year', F.col('policy_year').cast(IntegerType()))
    policy_earned_premium = policy_earned_premium.withColumn('earned_premium', F.col('earned_premium').cast(DecimalType(38, 2)))
    
    return {'silver.policy_earned_premium': policy_earned_premium}