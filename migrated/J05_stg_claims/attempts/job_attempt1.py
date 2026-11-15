from pyspark.sql import functions as F
from pyspark.sql.types import *

def run(spark, tables):
    claims = tables['dbo.claims']
    asof_date = '2025-12-31'
    
    stg_claims = claims.filter(F.upper(F.trim(claims['status'])) != 'REJECTED')\
        .select(
            claims['claim_id'],
            claims['policy_id'],
            claims['loss_date'],
            claims['reported_date'],
            claims['claim_type'],
            claims['status'],
            claims['reserve_amount'],
            F.datediff(F.lit(asof_date), claims['reported_date']).alias('claim_age_days')
        )
    
    return {'stg.claims': stg_claims}