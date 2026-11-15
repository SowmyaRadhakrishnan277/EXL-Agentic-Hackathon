from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def run(spark, tables):
    claims_df = tables['dbo.claims']
    asof_date = '2025-12-31'
    
    # Filter out rejected claims and NULL values in status
    filtered_claims_df = claims_df.filter((F.upper(F.trim(claims_df.status)) != 'REJECTED') & (claims_df.status.isNotNull()))
    
    # Calculate claim_age_days
    claim_age_days_df = filtered_claims_df.withColumn(
        'claim_age_days',
        F.datediff(F.lit(asof_date), claims_df.reported_date)
    )
    
    # Select and return the target DataFrame
    target_df = claim_age_days_df.select(
        'claim_id',
        'policy_id',
        'loss_date',
        'reported_date',
        'claim_type',
        'status',
        'reserve_amount',
        'claim_age_days'
    )
    
    return {'silver.claims': target_df}