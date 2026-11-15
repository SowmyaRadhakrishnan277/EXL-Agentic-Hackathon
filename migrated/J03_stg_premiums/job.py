from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

def run(spark, tables):
    pm = tables['dbo.premiums']
    
    # Ensure amount is of DecimalType(18,3)
    pm = pm.withColumn('amount', F.col('amount').cast(DecimalType(18, 3)))
    
    # Compute paid_flag
    pm = pm.withColumn('paid_flag', F.when(F.col('paid_date').isNull(), 'N').otherwise('Y'))
    
    # Select distinct rows
    stg_premiums = pm.select('premium_id', 'policy_id', 'due_date', 'paid_date', 'amount', 'paid_flag').distinct()
    
    return {'silver.premiums': stg_premiums}