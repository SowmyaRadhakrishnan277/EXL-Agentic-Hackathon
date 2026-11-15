from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, trim, datediff, year, col, lit


def run(spark, tables):
    # Constants
    asof_date = '2025-06-30'
    
    # Load input table
    customers_df = tables['dbo.customers']
    
    # Transformations
    transformed_df = customers_df.select(
        col('customer_id'),
        upper(trim(col('county'))).alias('county'),
        col('date_of_birth'),
        (year(lit(asof_date)) - year(col('date_of_birth'))).alias('age_years'),
        col('customer_since'),
        col('segment')
    )
    
    # Return the result as a dictionary
    return {'silver.customers': transformed_df}
