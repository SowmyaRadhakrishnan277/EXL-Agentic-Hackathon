from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

def run(spark, tables):
    stg_claim_payments = tables['legacy.stg_claim_payments']
    stg_claims = tables['stg.claims']

    # Aggregate payments by claim, treating NULL amounts as zero
    pay_by_claim = stg_claim_payments.groupBy('claim_id').agg(
        F.sum(F.coalesce(F.when(F.col('payment_type') == 'INDEMNITY', F.col('amount')), F.lit(0))).alias('indemnity_paid'),
        F.sum(F.coalesce(F.when(F.col('payment_type') == 'EXPENSE', F.col('amount')), F.lit(0))).alias('expense_paid'),
        F.count('*').alias('payment_count')
    )

    # Coalesce payment_count to 0 before joining
    pay_by_claim = pay_by_claim.withColumn('payment_count', F.coalesce(F.col('payment_count'), F.lit(0)))

    # Join with claims data
    joined = stg_claims.join(pay_by_claim, 'claim_id', 'left')

    # Calculate total paid and small claim flag
    claim_totals = joined.withColumn('total_paid', F.round(F.col('indemnity_paid') + F.col('expense_paid'), 2))\
        .withColumn('small_claim_flag', F.when(F.col('total_paid').isNull(), 'Y').otherwise(F.when(F.col('total_paid') < 500, 'Y').otherwise('N')))

    # Ensure output matches target schema
    claim_totals = claim_totals.select(
        'claim_id',
        'policy_id',
        'claim_type',
        'loss_date',
        F.col('indemnity_paid').cast(DecimalType(38, 3)),
        F.col('expense_paid').cast(DecimalType(38, 3)),
        F.col('payment_count').cast('bigint'),
        'small_claim_flag',
        F.col('total_paid').cast(DecimalType(18, 2))
    )

    return {'silver.claim_totals': claim_totals}