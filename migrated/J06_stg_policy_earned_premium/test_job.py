from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from migrated.J06_stg_policy_earned_premium.job import run

def test_job():
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    
    # Define schema for stg.policies
    schema = StructType([
        StructField("policy_id", StringType(), True),
        StructField("product_code", StringType(), True),
        StructField("annual_premium", DecimalType(38, 6), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True)
    ])
    
    # Create test data
    data = [
        ("P001", "PC01", 1000.00, "2021-01-01", "2021-12-31"),
        ("P002", "PC02", 2000.00, "2022-06-01", "2022-12-31"),
        ("P003", "PC03", 1500.00, "2023-01-01", None),
        ("P004", "PC04", 3000.00, None, "2024-12-31"),
        ("P005", "PC05", 2500.00, "2025-01-01", "2025-06-30")
    ]
    
    # Create DataFrame
    policies_df = spark.createDataFrame(data, schema)
    
    # Run the job
    tables = {"stg.policies": policies_df}
    result = run(spark, tables)
    
    # Define expected schema for result
    expected_schema = StructType([
        StructField("policy_id", StringType(), True),
        StructField("product_code", StringType(), True),
        StructField("policy_year", IntegerType(), True),
        StructField("days_in_force", IntegerType(), True),
        StructField("earned_premium", DecimalType(38, 2), True)
    ])
    
    # Define expected data
    expected_data = [
        ("P001", "PC01", 2021, 365, 1000.00),
        ("P002", "PC02", 2022, 183, 1000.00),
        ("P003", "PC03", 2023, 365, 1500.00),
        ("P004", "PC04", 2024, 365, 3000.00),
        ("P005", "PC05", 2025, 182, 1250.00)
    ]
    
    # Create expected DataFrame
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    # Check if the result matches the expected DataFrame
    assert result['silver.policy_earned_premium'].schema == expected_df.schema, "Schema mismatch"
    assert result['silver.policy_earned_premium'].collect() == expected_df.collect(), "Data mismatch"
    
    # Stop the Spark session
    spark.stop()
