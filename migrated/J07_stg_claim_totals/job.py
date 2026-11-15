from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def run(spark, tables):
    claims = tables['stg.claims']
    payments = tables['legacy.stg_claim_payments']

    # SAS/SQL string comparison ignores case and trailing blanks
    ptype = F.upper(F.rtrim(F.col('payment_type')))

    # Aggregate payments by claim: SUM(CASE WHEN ... THEN amount END) -> NULL when no rows match
    pay_by_claim = payments.groupBy('claim_id').agg(
        F.sum(F.when(ptype == 'INDEMNITY', F.col('amount'))).alias('indemnity_paid'),
        F.sum(F.when(ptype == 'EXPENSE', F.col('amount'))).alias('expense_paid'),
        F.count(F.lit(1)).alias('payment_count')
    )

    # LEFT JOIN keeps every claim; coalesce(payment_count, 0) applied AFTER the join
    joined = claims.join(pay_by_claim, 'claim_id', 'left')

    # SAS: a + b is missing if either operand is missing -> no coalesce before the addition
    total_paid = F.round(F.col('indemnity_paid') + F.col('expense_paid'), 2)

    out = (
        joined
        .withColumn('total_paid', total_paid.cast(DecimalType(18, 2)))
        .withColumn('payment_count', F.coalesce(F.col('payment_count'), F.lit(0)).cast('bigint'))
        .withColumn(
            'small_claim_flag',
            F.when(
                F.col('total_paid').isNull() | (F.col('total_paid') < F.lit(500)),
                F.lit('Y')
            ).otherwise(F.lit('N'))
        )
    )

    out = out.select(
        F.col('claim_id').cast('string').alias('claim_id'),
        F.col('policy_id').cast('string').alias('policy_id'),
        F.col('claim_type').cast('string').alias('claim_type'),
        F.col('loss_date').cast('date').alias('loss_date'),
        F.col('indemnity_paid').cast(DecimalType(38, 3)).alias('indemnity_paid'),
        F.col('expense_paid').cast(DecimalType(38, 3)).alias('expense_paid'),
        F.col('payment_count').alias('payment_count'),
        F.col('small_claim_flag').alias('small_claim_flag'),
        F.col('total_paid').alias('total_paid')
    )

    return {'silver.claim_totals': out}
