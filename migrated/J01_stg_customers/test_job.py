from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from pyspark.sql.functions import lit, upper, trim, year
from migrated.J01_stg_customers.job import run

def test_run(spark):
    # Define the schema for the input data
    schema = StructType([
        StructField('customer_id', StringType(), True),
        StructField('county', StringType(), True),
        StructField('date_of_birth', DateType(), True),
        StructField('customer_since', DateType(), True),
        StructField('segment', StringType(), True)
    ])

    # Create a DataFrame with test data
    data = [
        ('C001', '  county1 ', '1980-01-01', '2000-01-01', 'A'),
        ('C002', 'county2', '1990-01-01', '2010-01-01', 'B'),
        ('C003', 'county3', '2000-01-01', '2020-01-01', 'C'),
        ('C004', 'county4', '2010-01-01', '2030-01-01', 'D')
    ]

    customers_df = spark.createDataFrame(data, schema)

    # Define the tables dictionary
    tables = {
        'dbo.customers': customers_df
    }

    # Run the job
    result = run(spark, tables)

    # Define the expected schema for the output data
    expected_schema = StructType([
        StructField('customer_id', StringType(), True),
        StructField('county', StringType(), True),
        StructField('date_of_birth', DateType(), True),
        StructField('age_years', IntegerType(), True),
        StructField('customer_since', DateType(), True),
        StructField('segment', StringType(), True)
    ])

    # Define the expected data
    expected_data = [
        ('C001', 'COUNTY1', '1980-01-01', 45, '2000-01-01', 'A'),
        ('C002', 'COUNTY2', '1990-01-01', 35, '2010-01-01', 'B'),
        ('C003', 'COUNTY3', '2000-01-01', 25, '2020-01-01', 'C'),
        ('C004', 'COUNTY4', '2010-01-01', 15, '2030-01-01', 'D')
    ]

    # Create a DataFrame with the expected data
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Check if the result matches the expected output
    assert result['silver.customers'].schema == expected_df.schema, 'Schema mismatch'
    assert result['silver.customers'].collect() == expected_df.collect(), 'Data mismatch'

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[1]').appName('test_J01_stg_customers').getOrCreate()
    test_run(spark)
    spark.stop()