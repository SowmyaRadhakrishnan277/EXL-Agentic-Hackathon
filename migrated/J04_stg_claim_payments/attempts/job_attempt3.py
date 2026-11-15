from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

def run(spark, tables):
    claim_payments = tables['dbo.claim_payments']
    
    # Deduplicate by payment_id, retaining the first occurrence
    window_spec = Window.orderBy(F.monotonically_increasing_id())
    dedup = claim_payments.withColumn('row_num', F.row_number().over(window_spec.partitionBy('payment_id'))).filter(F.col('row_num') == 1).drop('row_num')
    
    # Categorize payments into size bands, treating NULL amounts as 'SMALL'
    stg_claim_payments = dedup.withColumn('amount_band', 
        F.when(F.col('amount').isNull(), 'SMALL')
        .when(F.col('amount') < 100, 'SMALL')
        .when(F.col('amount') < 1000, 'MEDIUM')
        .otherwise('LARGE'))
    
    # Ensure the output matches the target data contract
    stg_claim_payments = stg_claim_payments.select(
        F.col('payment_id').cast(StringType()),
        F.col('claim_id').cast(StringType()),
        F.col('payment_date').cast(DateType()),
        F.col('payment_type').cast(StringType()),
        F.col('amount').cast(DecimalType(18, 3)),
        F.col('amount_band').cast(StringType())
    )
    
    return {'silver.claim_payments': stg_claim_payments}