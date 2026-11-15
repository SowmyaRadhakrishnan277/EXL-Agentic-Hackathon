from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col
from migrated.J03_stg_premiums.job import run

def test_run(spark):
    # Create test data
    data = [
        (1, 101, '2023-01-01', None, 100.000),
        (2, 102, '2023-01-02', '2023-01-02', 200.000),
        (3, 101, '2023-01-01', None, 100.000),
        (4, 103, '2023-01-03', '2023-01-03', 300.000),
        (5, 104, '2023-01-04', None, 400.000)
    ]
    columns = ['premium_id', 'policy_id', 'due_date', 'paid_date', 'amount']
    pm_df = spark.createDataFrame(data, columns)
    pm_df = pm_df.withColumn('amount', col('amount').cast(DecimalType(18, 3)))
    tables = {'dbo.premiums': pm_df}
    
    # Run the job
    result = run(spark, tables)
    stg_premiums = result['silver.premiums']
    
    # Define expected data
    expected_data = [
        (1, 101, '2023-01-01', None, 100.000, 'N'),
        (2, 102, '2023-01-02', '2023-01-02', 200.000, 'Y'),
        (4, 103, '2023-01-03', '2023-01-03', 300.000, 'Y'),
        (5, 104, '2023-01-04', None, 400.000, 'N')
    ]
    expected_columns = ['premium_id', 'policy_id', 'due_date', 'paid_date', 'amount', 'paid_flag']
    expected_df = spark.createDataFrame(expected_data, expected_columns)
    expected_df = expected_df.withColumn('amount', col('amount').cast(DecimalType(18, 3)))
    
    # Check if the result matches the expected output
    assert stg_premiums.collect() == expected_df.collect()
    assert stg_premiums.schema == expected_df.schema
    
if __name__ == '__main__':
    spark = SparkSession.builder.master('local[1]').appName('test_J03_stg_premiums').getOrCreate()
    test_run(spark)
    spark.stop()