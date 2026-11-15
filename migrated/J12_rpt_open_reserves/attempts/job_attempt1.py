from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

def run(spark, tables):
    claims = tables['stg.claims']
    policies = tables['stg.policies']
    
    # Filter claims with status 'OPEN' (case-insensitive)
    open_claims = claims.filter(F.upper(F.trim(claims.status)) == 'OPEN')
    
    # Join claims with policies
    joined_df = open_claims.join(policies, on='policy_id')
    
    # Define age_band with handling for NULL claim_age_days
    age_band = F.when(claims.claim_age_days <= 30, '0-30')\
               .when(claims.claim_age_days <= 90, '31-90')\
               .when(claims.claim_age_days <= 180, '91-180')\
               .otherwise('180+')
    
    # Aggregate data
    result_df = joined_df.groupBy(policies.product_code, age_band)\
        .agg(
            F.count(F.lit(1)).alias('open_claims'),
            F.count(claims.reserve_amount).alias('claims_with_reserve'),
            F.sum(claims.reserve_amount).cast(DecimalType(38, 2)).alias('total_reserve')
        )
    
    return {'gold.open_reserves': result_df}