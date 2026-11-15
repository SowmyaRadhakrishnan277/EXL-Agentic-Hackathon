from pyspark.sql import functions as F
from pyspark.sql.types import *

def run(spark, tables):
    policies = tables['dbo.policies']
    products = tables['dbo.products']
    
    # Ensure product_code is treated case-insensitively by converting to upper
    policies = policies.withColumn('product_code', F.upper(F.col('product_code')))
    products = products.withColumn('product_code', F.upper(F.col('product_code')))
    
    # Join policies with products
    joined_df = policies.join(products, on='product_code', how='inner')
    
    # Apply COALESCE for end_date and default to '9999-12-31'
    joined_df = joined_df.withColumn('end_date', F.coalesce(F.col('end_date'), F.to_date(F.lit('9999-12-31'))))
    
    # Create rolling_flag based on end_date
    joined_df = joined_df.withColumn('rolling_flag', F.when(F.col('end_date').isNull(), 'Y').otherwise('N'))
    
    # Standardize status by trimming and converting to upper
    joined_df = joined_df.withColumn('status', F.upper(F.rtrim(F.col('status'))))
    
    # Select and return the final DataFrame
    result_df = joined_df.select(
        'policy_id',
        'customer_id',
        'product_code',
        'line_of_business',
        'broker_id',
        'channel',
        'start_date',
        'end_date',
        'rolling_flag',
        'status',
        'annual_premium',
        'payment_frequency'
    )
    
    return {'silver.policies': result_df}