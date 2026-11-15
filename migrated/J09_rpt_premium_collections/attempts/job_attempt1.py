from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

def run(spark, tables):
    premiums = tables['stg.premiums']
    
    # Calculate due_month
    premiums = premiums.withColumn('due_month', F.date_trunc('month', F.col('due_date')))
    
    # Aggregate data
    result = premiums.groupBy('due_month').agg(
        F.count(F.col('*')).alias('instalments_due'),
        F.sum(F.col('amount')).alias('amount_due'),
        F.sum(F.when(F.col('paid_flag') == 'Y', F.col('amount')).otherwise(0)).alias('amount_collected'),
        (F.sum(F.col('amount')) - F.sum(F.when(F.col('paid_flag') == 'Y', F.col('amount')).otherwise(0))).alias('arrears'),
        (F.lit(100.0) * F.sum(F.when(F.col('paid_flag') == 'Y', F.col('amount')).otherwise(0)) / F.when(F.col('amount_due') == 0, F.lit(None)).otherwise(F.col('amount_due'))).cast(DecimalType(9, 2)).alias('collection_rate_pct')
    )
    
    # Ensure all columns are not null
    result = result.withColumn('instalments_due', F.coalesce(F.col('instalments_due'), F.lit(0)))
    result = result.withColumn('amount_due', F.coalesce(F.col('amount_due'), F.lit(0.0)))
    result = result.withColumn('amount_collected', F.coalesce(F.col('amount_collected'), F.lit(0.0)))
    result = result.withColumn('arrears', F.coalesce(F.col('arrears'), F.lit(0.0)))
    result = result.withColumn('collection_rate_pct', F.coalesce(F.col('collection_rate_pct'), F.lit(0.0)))
    
    return {'gold.premium_collections': result}
