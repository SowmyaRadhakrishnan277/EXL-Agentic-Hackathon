from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def run(spark, tables):
    customers = tables['dbo.customers']
    asof_date = '2025-06-30'
    
    stg_customers = customers.select(
        'customer_id',
        F.upper(F.trim(F.col('county'))).alias('county'),
        'date_of_birth',
        (F.year(F.lit(asof_date)) - F.year('date_of_birth')).alias('age_years'),
        'customer_since',
        'segment'
    )
    
    return {'silver.customers': stg_customers}