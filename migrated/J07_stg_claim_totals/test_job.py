from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.functions import lit, col, when, round, coalesce
from migrated.J07_stg_claim_totals.job import run

def create_test_data(spark):
    # Create test data for stg_claim_payments
    payments_data = [
        ('C1', 'P1', 'INDEMNITY', 100.0),
        ('C1', 'P1', 'EXPENSE', 50.0),
        ('C2', 'P2', 'INDEMNITY', 200.0),
        ('C3', 'P3', 'EXPENSE', 75.0),
        ('C4', None, 'INDEMNITY', 150.0),
    ]
    payments_schema = StructType([
        StructField('claim_id', StringType(), True),
        StructField('policy_id', StringType(), True),
        StructField('payment_type', StringType(), True),
        StructField('amount', DecimalType(10, 2), True),
    ])
    payments_df = spark.createDataFrame(payments_data, payments_schema)

    # Create test data for stg_claims
    claims_data = [
        ('C1', 'P1', 'Type1', '2023-01-01', '2023-01-02'),
        ('C2', 'P2', 'Type2', '2023-01-03', '2023-01-04'),
        ('C3', 'P3', 'Type3', '2023-01-05', '2023-01-06'),
        ('C4', None, 'Type4', '2023-01-07', '2023-01-08'),
    ]
    claims_schema = StructType([
        StructField('claim_id', StringType(), True),
        StructField('policy_id', StringType(), True),
        StructField('claim_type', StringType(), True),
        StructField('loss_date', StringType(), True),
        StructField('reported_date', StringType(), True),
    ])
    claims_df = spark.createDataFrame(claims_data, claims_schema)
    claims_df = claims_df.withColumn('loss_date', col('loss_date').cast(DateType()))
    claims_df = claims_df.withColumn('reported_date', col('reported_date').cast(DateType()))

    return {'legacy.stg_claim_payments': payments_df, 'stg.claims': claims_df}

def test_job():
    spark = SparkSession.builder.master('local[1]').appName('test_job').getOrCreate()
    tables = create_test_data(spark)
    result = run(spark, tables)
    claim_totals_df = result['silver.claim_totals']

    # Check the number of rows
    assert claim_totals_df.count() == 4

    # Check the schema
    expected_schema = StructType([
        StructField('claim_id', StringType(), True),
        StructField('policy_id', StringType(), True),
        StructField('claim_type', StringType(), True),
        StructField('loss_date', DateType(), True),
        StructField('indemnity_paid', DecimalType(38, 3), True),
        StructField('expense_paid', DecimalType(38, 3), True),
        StructField('payment_count', IntegerType(), True),
        StructField('small_claim_flag', StringType(), True),
        StructField('total_paid', DecimalType(18, 2), True),
    ])
    assert claim_totals_df.schema == expected_schema

    # Check specific values
    claim_totals_df.show()
    assert claim_totals_df.filter(col('claim_id') == 'C1').select('total_paid').collect()[0][0] == Decimal(150.00)
    assert claim_totals_df.filter(col('claim_id') == 'C2').select('total_paid').collect()[0][0] == Decimal(200.00)
    assert claim_totals_df.filter(col('claim_id') == 'C3').select('total_paid').collect()[0][0] == Decimal(75.00)
    assert claim_totals_df.filter(col('claim_id') == 'C4').select('total_paid').collect()[0][0] == Decimal(150.00)

    # Check small_claim_flag
    assert claim_totals_df.filter(col('claim_id') == 'C1').select('small_claim_flag').collect()[0][0] == 'N'
    assert claim_totals_df.filter(col('claim_id') == 'C2').select('small_claim_flag').collect()[0][0] == 'N'
    assert claim_totals_df.filter(col('claim_id') == 'C3').select('small_claim_flag').collect()[0][0] == 'Y'
    assert claim_totals_df.filter(col('claim_id') == 'C4').select('small_claim_flag').collect()[0][0] == 'N'

    spark.stop()

if __name__ == '__main__':
    test_job()
