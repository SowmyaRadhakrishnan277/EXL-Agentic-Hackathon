from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.functions import lit
from migrated.J11_rpt_broker_commission.job import run

def create_test_data(spark):
    brokers_schema = StructType([
        StructField('broker_id', StringType(), True),
        StructField('active_flag', StringType(), True),
        StructField('commission_rate', DecimalType(5, 4), True)
    ])
    brokers_data = [
        ('B001', 'Y', 0.05),
        ('B002', 'N', 0.05),
        ('B003', 'Y', 0.03)
    ]
    brokers_df = spark.createDataFrame(brokers_data, brokers_schema)

    policies_schema = StructType([
        StructField('broker_id', StringType(), True),
        StructField('status', StringType(), True),
        StructField('start_date', DateType(), True),
        StructField('annual_premium', DecimalType(10, 2), True)
    ])
    policies_data = [
        ('B001', 'OPEN', '2022-01-01', 1000.00),
        ('B001', 'CANCELLED', '2022-02-01', 2000.00),
        ('B003', 'OPEN  ', '2023-03-01', 1500.00),
        ('B004', 'OPEN', '2024-04-01', 1200.00),
        ('B001', 'OPEN', '2025-05-01', 1800.00)
    ]
    policies_df = spark.createDataFrame(policies_data, policies_schema)

    return {'dbo.brokers': brokers_df, 'stg.policies': policies_df}

def test_job():
    spark = SparkSession.builder.master('local[1]').appName('test_job').getOrCreate()
    tables = create_test_data(spark)
    result = run(spark, tables)
    result_df = result['gold.broker_commission']

    # Expected data
    expected_data = [
        ('B001', 2022, 1, 1000.00, 50.00),
        ('B003', 2023, 1, 1500.00, 45.00),
        ('B001', 2025, 1, 1800.00, 90.00)
    ]
    expected_schema = StructType([
        StructField('broker_id', StringType(), True),
        StructField('written_year', IntegerType(), True),
        StructField('policies_written', IntegerType(), True),
        StructField('written_premium', DecimalType(38, 2), True),
        StructField('commission_due', DecimalType(38, 2), True)
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Check if the result matches the expected data
    assert result_df.collect() == expected_df.collect()

if __name__ == '__main__':
    test_job()
