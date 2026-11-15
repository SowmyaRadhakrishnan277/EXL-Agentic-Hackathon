from pyspark.sql import SparkSession
from pyspark.sql.types import *
from migrated.J04_stg_claim_payments.job import run

def test_run(spark):
    # Sample data
    data = [
        ('123456789012', 'C123456789012', '2023-01-01', 'TYPE1', 50),
        ('123456789012', 'C123456789012', '2023-01-01', 'TYPE1', 50),
        ('234567890123', 'C234567890123', '2023-02-01', 'TYPE2', 500),
        ('345678901234', 'C345678901234', '2023-03-01', 'TYPE3', 5000),
        ('456789012345', 'C456789012345', '2023-04-01', 'TYPE4', None)
    ]
    schema = StructType([
        StructField('payment_id', StringType(), True),
        StructField('claim_id', StringType(), True),
        StructField('payment_date', StringType(), True),
        StructField('payment_type', StringType(), True),
        StructField('amount', DecimalType(18, 3), True)
    ])
    claim_payments = spark.createDataFrame(data, schema)
    tables = {'dbo.claim_payments': claim_payments}
    result = run(spark, tables)
    silver_claim_payments = result['silver.claim_payments']
    silver_claim_payments.show()
    # Check for deduplication and correct amount_band
    assert silver_claim_payments.count() == 5
    assert silver_claim_payments.filter(F.col('payment_id') == '123456789012').count() == 1
    assert silver_claim_payments.filter(F.col('payment_id') == '234567890123').count() == 1
    assert silver_claim_payments.filter(F.col('payment_id') == '345678901234').count() == 1
    assert silver_claim_payments.filter(F.col('payment_id') == '456789012345').count() == 1
    # Check for correct amount_band categorization
    assert silver_claim_payments.filter(F.col('amount_band') == 'SMALL').count() == 2
    assert silver_claim_payments.filter(F.col('amount_band') == 'MEDIUM').count() == 1
    assert silver_claim_payments.filter(F.col('amount_band') == 'LARGE').count() == 1
    # Check for correct data types
    assert silver_claim_payments.schema['payment_id'].dataType == StringType()
    assert silver_claim_payments.schema['claim_id'].dataType == StringType()
    assert silver_claim_payments.schema['payment_date'].dataType == DateType()
    assert silver_claim_payments.schema['payment_type'].dataType == StringType()
    assert silver_claim_payments.schema['amount'].dataType == DecimalType(18, 3)
    assert silver_claim_payments.schema['amount_band'].dataType == StringType()

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[1]').appName('test_J04_stg_claim_payments').getOrCreate()
    test_run(spark)
    spark.stop()