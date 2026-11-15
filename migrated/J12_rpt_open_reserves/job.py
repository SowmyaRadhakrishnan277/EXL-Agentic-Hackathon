from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def run(spark, tables):
    claims = tables['stg.claims']
    policies = tables['stg.policies']

    c = claims.alias('c')
    p = policies.alias('p')

    # INNER JOIN stg.policies p ON p.policy_id = c.policy_id
    # WHERE c.status = 'OPEN'  -> T-SQL is case-insensitive and ignores trailing
    # spaces only (leading spaces still matter), so upper(rtrim(...)) on both sides.
    joined = c.join(p, F.col('p.policy_id') == F.col('c.policy_id'), 'inner') \
              .where(F.upper(F.rtrim(F.col('c.status'))) == F.lit('OPEN'))

    # CASE ... END AS age_band. A NULL claim_age_days makes every WHEN unknown,
    # so T-SQL falls through to ELSE '180+' - Spark's when/otherwise does the same.
    banded = joined \
        .withColumn('product_code', F.col('p.product_code')) \
        .withColumn(
            'age_band',
            F.when(F.col('c.claim_age_days') <= 30, F.lit('0-30'))
             .when(F.col('c.claim_age_days') <= 90, F.lit('31-90'))
             .when(F.col('c.claim_age_days') <= 180, F.lit('91-180'))
             .otherwise(F.lit('180+'))
        )

    result = banded.groupBy('product_code', 'age_band').agg(
        F.count(F.lit(1)).cast('bigint').alias('open_claims'),
        F.count(F.col('c.reserve_amount')).cast('bigint').alias('claims_with_reserve'),
        F.sum(F.col('c.reserve_amount')).cast(DecimalType(38, 2)).alias('total_reserve')
    ).select(
        'product_code',
        'age_band',
        'open_claims',
        'claims_with_reserve',
        'total_reserve'
    )

    return {'gold.open_reserves': result}
