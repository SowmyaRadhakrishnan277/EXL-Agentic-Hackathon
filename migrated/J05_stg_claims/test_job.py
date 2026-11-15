from pyspark.sql import SparkSession
from pyspark.sql.types import *
from migrated.J05_stg_claims.job import run

def test_run(spark):
    # Sample data
    data = [
        (1, 101, '2023-01-01', '2023-01-01', 'TypeA', 'OPEN', 1000.0),
        (2, 102, '2023-02-01', '2023-02-01', 'TypeB', 'REJECTED', 2000.0),
        (3, 103, '2023-03-01', '2023-03-01', 'TypeC', 'open  ', 3000.0),
        (4, 104, '2023-04-01', '2023-04-01', 'TypeD', '  OPEN', 4000.0),
        (5, 105, '2023-05-01', '2023-05-01', 'TypeE', 'CLOSED', 5000.0)
    ]
    schema = StructType([
        StructField('claim_id', IntegerType()),
        StructField('policy_id', IntegerType()),
        StructField('loss_date', StringType()),
        StructField('reported_date', StringType()),
        StructField('claim_type', StringType()),
        StructField('status', StringType()),
        StructField('reserve_amount', DoubleType())
    ])
    claims_df = spark.createDataFrame(data, schema)
    tables = {'dbo.claims': claims_df}
    result = run(spark, tables)
    stg_claims_df = result['stg.claims']
    
    # Check the output
    expected_data = [
        (1, 101, '2023-01-01', '2023-01-01', 'TypeA', 'OPEN', 1000.0, 364),
        (3, 103, '2023-03-01', '2023-03-01', 'TypeC', 'open  ', 3000.0, 334),
        (4, 104, '2023-04-01', '2023-04-01', 'TypeD', '  OPEN', 4000.0, 304),
        (5, 105, '2023-05-01', '2023-05-01', 'TypeE', 'CLOSED', 5000.0, 274)
    ]
    expected_schema = StructType([
        StructField('claim_id', IntegerType()),
        StructField('policy_id', IntegerType()),
        StructField('loss_date', StringType()),
        StructField('reported_date', StringType()),
        StructField('claim_type', StringType()),
        StructField('status', StringType()),
        StructField('reserve_amount', DoubleType()),
        StructField('claim_age_days', IntegerType())
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    assert stg_claims_df.collect() == expected_df.collect()

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[1]').getOrCreate()
    test_run(spark)
    spark.stop()