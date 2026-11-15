from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

def run(spark, tables):
    claim_payments = tables['dbo.claim_payments']
    claims = tables['stg.claims']
    policies = tables['stg.policies']

    # Subquery to deduplicate claim_payments
    cp = claim_payments.select('payment_id', 'claim_id', 'payment_date', 'payment_type', 'amount')
    cp = cp.dropDuplicates()

    # Join with claims and policies
    df = cp.join(claims, on='claim_id')
    df = df.join(policies, on='policy_id')

    # Filter for indemnity payments and non-null payment_date
    df = df.filter((F.upper(F.trim(df['payment_type'])) == 'INDEMNITY') & (df['payment_date'].isNotNull()))

    # Aggregate by product_code and payment_month
    result = df.groupBy(
        policies['product_code'],
        F.trunc(df['payment_date'], 'month').alias('payment_month')
    ).agg(
        F.countDistinct(df['claim_id']).alias('claims_paid'),
        F.count('*').alias('payments'),
        F.round(F.sum(df['amount']).cast(DecimalType(38, 2)), 2).alias('indemnity_paid')
    )

    return {'gold.monthly_claims_paid': result}
