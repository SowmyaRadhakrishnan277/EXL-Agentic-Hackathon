from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType
from migrated.j02_stg_policies.job import run

def test_j02_stg_policies():
    spark = SparkSession.builder.master('local[1]').getOrCreate()

    # Sample data for dbo.policies
    policies_data = [
        ('P1', 'C1', 'PR1', '2020-01-01', None, 'B1', 'CH1', 1000.00, 'Monthly'),
        ('P2', 'C2', 'PR2', '2021-01-01', '2025-12-31', 'B2', 'CH2', 2000.00, 'Annual'),
        ('P3', 'C3', 'PR3', '2022-01-01', None, 'B3', 'CH3', 3000.00, 'Quarterly')
    ]
    policies_schema = StructType([
        StructField('policy_id', StringType(), True),
        StructField('customer_id', StringType(), True),
        StructField('product_code', StringType(), True),
        StructField('start_date', DateType(), True),
        StructField('end_date', DateType(), True),
        StructField('broker_id', StringType(), True),
        StructField('channel', StringType(), True),
        StructField('annual_premium', DecimalType(18, 2), True),
        StructField('payment_frequency', StringType(), True)
    ])
    policies_df = spark.createDataFrame(policies_data, policies_schema)

    # Sample data for dbo.products
    products_data = [
        ('PR1', 'LOB1'),
        ('PR2', 'LOB2'),
        ('PR3', 'LOB3')
    ]
    products_schema = StructType([
        StructField('product_code', StringType(), True),
        StructField('line_of_business', StringType(), True)
    ])
    products_df = spark.createDataFrame(products_data, products_schema)

    # Run the job
    tables = {'dbo.policies': policies_df, 'dbo.products': products_df}
    result = run(spark, tables)

    # Verify the output
    expected_data = [
        ('P1', 'C1', 'PR1', 'LOB1', 'B1', 'CH1', '2020-01-01', '9999-12-31', 'Y', 'OPEN', 1000.00, 'Monthly'),
        ('P2', 'C2', 'PR2', 'LOB2', 'B2', 'CH2', '2021-01-01', '2025-12-31', 'N', 'OPEN', 2000.00, 'Annual'),
        ('P3', 'C3', 'PR3', 'LOB3', 'B3', 'CH3', '2022-01-01', '9999-12-31', 'Y', 'OPEN', 3000.00, 'Quarterly')
    ]
    expected_schema = StructType([
        StructField('policy_id', StringType(), True),
        StructField('customer_id', StringType(), True),
        StructField('product_code', StringType(), True),
        StructField('line_of_business', StringType(), True),
        StructField('broker_id', StringType(), True),
        StructField('channel', StringType(), True),
        StructField('start_date', DateType(), True),
        StructField('end_date', DateType(), True),
        StructField('rolling_flag', StringType(), True),
        StructField('status', StringType(), True),
        StructField('annual_premium', DecimalType(18, 2), True),
        StructField('payment_frequency', StringType(), True)
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    assert result['silver.policies'].collect() == expected_df.collect()
