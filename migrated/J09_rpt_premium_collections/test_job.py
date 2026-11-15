from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType
from pyspark.sql.functions import lit, col, when, sum, count, date_trunc, coalesce
from migrated.J09_rpt_premium_collections.job import run

def test_run(spark):
    # Create a sample DataFrame for stg.premiums
    data = [
        (None, 100.0, 'Y'),
        ('2023-01-15', 200.0, 'N'),
        ('2023-01-15', 300.0, 'Y'),
        ('2023-02-20', 400.0, 'N'),
        ('2023-02-20', 500.0, 'Y')
    ]
    schema = StructType([
        StructField('due_date', StringType(), True),
        StructField('amount', DecimalType(38, 3), True),
        StructField('paid_flag', StringType(), True)
    ])
    premiums_df = spark.createDataFrame(data, schema)
    tables = {'stg.premiums': premiums_df}
    
    # Run the job
    result = run(spark, tables)
    result_df = result['gold.premium_collections']
    
    # Define expected output
    expected_data = [
        (None, 1, 100.0, 0.0, 100.0, 0.0),
        ('2023-01-01', 2, 500.0, 300.0, 200.0, 60.0),
        ('2023-02-01', 2, 900.0, 500.0, 400.0, 55.56)
    ]
    expected_schema = StructType([
        StructField('due_month', DateType(), True),
        StructField('instalments_due', StringType(), True),
        StructField('amount_due', DecimalType(38, 3), True),
        StructField('amount_collected', DecimalType(38, 3), True),
        StructField('arrears', DecimalType(38, 3), True),
        StructField('collection_rate_pct', DecimalType(9, 2), True)
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    # Check if the result matches the expected output
    assert result_df.collect() == expected_df.collect()

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[1]').appName('test').getOrCreate()
    test_run(spark)
    spark.stop()