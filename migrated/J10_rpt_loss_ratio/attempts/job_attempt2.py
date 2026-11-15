from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

def run(spark, tables):
    claim_totals = tables['legacy.claim_totals']
    policies = tables['stg.policies']
    earned_policy = tables['stg.policy_earned_premium']

    # Create earned DataFrame
    earned = earned_policy.groupBy('product_code', 'policy_year')\
        .agg(F.sum('earned_premium').alias('earned_premium'))

    # Create incurred DataFrame
    incurred = claim_totals.join(policies, 'policy_id')\
        .groupBy('product_code', F.year('loss_date').alias('loss_year'))\
        .agg(F.sum('indemnity_paid').alias('incurred'), F.count('*').alias('claim_count'))

    # Create lr DataFrame
    lr = earned.alias('e').join(incurred.alias('i'),\
        (F.col('e.product_code') == F.col('i.product_code')) &\
        (F.col('e.policy_year') == F.col('i.loss_year')),\
        'outer'\
    )\
        .select(
            F.coalesce(F.col('e.product_code'), F.col('i.product_code')).alias('product_code'),
            F.coalesce(F.col('e.policy_year'), F.col('i.loss_year')).alias('year'),
            F.col('e.earned_premium'),
            F.col('i.incurred'),
            F.col('i.claim_count')
        )

    # Create legacy.rpt_loss_ratio DataFrame
    legacy_rpt_loss_ratio = lr.withColumn('loss_ratio',\
        F.when(F.col('earned_premium') > 0,\
            F.round(F.col('incurred') / F.col('earned_premium'), 4)
        )\
        .otherwise(F.lit(None))
    )

    # Cast columns to match target data contract
    legacy_rpt_loss_ratio = legacy_rpt_loss_ratio.select(
        F.col('product_code').cast('string'),
        F.col('year').cast('string'),
        F.col('earned_premium').cast(DecimalType(38, 2)),
        F.col('incurred').cast(DecimalType(38, 3)),
        F.col('claim_count').cast('bigint'),
        F.col('loss_ratio').cast(DecimalType(38, 6))
    )

    return {'gold.loss_ratio': legacy_rpt_loss_ratio}