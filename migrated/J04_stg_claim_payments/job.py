from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import *

def run(spark, tables):
    claim_payments = tables['dbo.claim_payments']
    
    # Deduplicate by payment_id, retaining the first occurrence
    w = Window.partitionBy('payment_id').orderBy(F.asc('payment_id'))
    dedup = claim_payments.withColumn('row_num', F.row_number().over(w)).filter(F.col('row_num') == 1).drop('row_num')
    
    # Categorize amounts into bands
    categorized = dedup.withColumn('amount_band', 
        F.when(F.col('amount').isNull(), 'SMALL')
        .when(F.col('amount') < 100, 'SMALL')
        .when(F.col('amount') < 1000, 'MEDIUM')
        .otherwise('LARGE'))
    
    # Ensure the output matches the target data contract
    result = categorized.select(
        F.col('payment_id').alias('payment_id'),
        F.col('claim_id').alias('claim_id'),
        F.col('payment_date').cast(DateType()).alias('payment_date'),
        F.col('payment_type').alias('payment_type'),
        F.col('amount').cast(DecimalType(18, 3)).alias('amount'),
        F.col('amount_band').alias('amount_band')
    )
    
    return {'silver.claim_payments': result}