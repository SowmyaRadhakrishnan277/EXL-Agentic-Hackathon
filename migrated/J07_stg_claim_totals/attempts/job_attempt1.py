from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

def run(spark, tables):
    claims = tables['stg.claims']
    payments = tables['legacy.stg_claim_payments']

    # Aggregate payments by claim
    pay_by_claim = payments.groupBy('claim_id').agg(
        F.sum(F.when(F.col('payment_type') == 'INDEMNITY', F.col('amount'))).alias('indemnity_paid'),
        F.sum(F.when(F.col('payment_type') == 'EXPENSE', F.col('amount'))).alias('expense_paid'),
        F.count('*').alias('payment_count')
    )

    # Join claims with aggregated payments
    joined = claims.join(pay_by_claim, 'claim_id', 'left')

    # Calculate total paid and small claim flag
    claim_totals = joined.withColumn('total_paid', F.round(F.col('indemnity_paid') + F.col('expense_paid'), 2))\
        .withColumn('small_claim_flag', F.when(F.col('total_paid') < 500, 'Y').otherwise('N'))\
        .withColumn('payment_count', F.coalesce(F.col('payment_count'), F.lit(0)))

    # Select and order columns to match the target schema
    claim_totals = claim_totals.select(
        'claim_id',
        'policy_id',
        'claim_type',
        'loss_date',
        'indemnity_paid',
        'expense_paid',
        'payment_count',
        'small_claim_flag',
        'total_paid'
    )

    return {'silver.claim_totals': claim_totals}
