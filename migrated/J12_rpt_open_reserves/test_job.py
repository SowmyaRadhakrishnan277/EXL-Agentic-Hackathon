from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from migrated.J12_rpt_open_reserves.job import run

def test_run(spark):
    # Sample data for stg.claims
    claims_data = [
        (1, 'OPEN', 10, 100.0),
        (2, 'OPEN  ', 50, 200.0),
        (3, 'CLOSED', 100, 300.0),
        (4, 'OPEN', None, 400.0),
        (5, 'OPEN', 200, None)
    ]
    claims_schema = StructType([
        StructField('claim_id', IntegerType(), True),
        StructField('status', StringType(), True),
        StructField('claim_age_days', IntegerType(), True),
        StructField('reserve_amount', DecimalType(38, 2), True)
    ])
    claims_df = spark.createDataFrame(claims_data, claims_schema)

    # Sample data for stg.policies
    policies_data = [
        (1, 'ProductA'),
        (2, 'ProductB'),
        (3, 'ProductC'),
        (4, 'ProductA'),
        (5, 'ProductB')
    ]
    policies_schema = StructType([
        StructField('policy_id', IntegerType(), True),
        StructField('product_code', StringType(), True)
    ])
    policies_df = spark.createDataFrame(policies_data, policies_schema)

    # Run the job
    tables = {
        'stg.claims': claims_df,
        'stg.policies': policies_df
    }
    result = run(spark, tables)
    open_reserves_df = result['gold.open_reserves']

    # Expected data
    expected_data = [
        ('ProductA', '0-30', 1, 1, 100.0),
        ('ProductA', '180+', 1, 1, 400.0),
        ('ProductB', '31-90', 1, 1, 200.0),
        ('ProductB', '180+', 1, 0, None)
    ]
    expected_schema = StructType([
        StructField('product_code', StringType(), True),
        StructField('age_band', StringType(), True),
        StructField('open_claims', IntegerType(), True),
        StructField('claims_with_reserve', IntegerType(), True),
        StructField('total_reserve', DecimalType(38, 2), True)
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Check if the result matches the expected output
    assert open_reserves_df.collect() == expected_df.collect()

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[1]').appName('test_J12_rpt_open_reserves').getOrCreate()
    test_run(spark)
    spark.stop()
