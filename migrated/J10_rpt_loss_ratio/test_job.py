from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DecimalType, DateType
from pyspark.sql.functions import lit, col, year, sum, count, round, coalesce
from migrated.J10_rpt_loss_ratio.job import run

def create_dataframe(data, schema):
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    return spark.createDataFrame(data, schema)

def test_case_1():
    # Test case for case-insensitive string comparison and trailing spaces
    claim_totals_data = [
        (1, 'open  ', '2020-01-01'),
        (2, 'rejected', '2020-01-02'),
        (3, 'open', '2020-01-03')
    ]
    claim_totals_schema = StructType([
        StructField('policy_id', IntegerType()),
        StructField('status', StringType()),
        StructField('loss_date', DateType())
    ])
    claim_totals = create_dataframe(claim_totals_data, claim_totals_schema)

    policies_data = [
        (1, 'policy1'),
        (2, 'policy2'),
        (3, 'policy3')
    ]
    policies_schema = StructType([
        StructField('policy_id', IntegerType()),
        StructField('product_code', StringType())
    ])
    policies = create_dataframe(policies_data, policies_schema)

    earned_policy_data = [
        (1, 'product1', 2020, 1000),
        (2, 'product2', 2020, 2000),
        (3, 'product3', 2020, 3000)
    ]
    earned_policy_schema = StructType([
        StructField('policy_id', IntegerType()),
        StructField('product_code', StringType()),
        StructField('policy_year', IntegerType()),
        StructField('earned_premium', IntegerType())
    ])
    earned_policy = create_dataframe(earned_policy_data, earned_policy_schema)

    tables = {
        'legacy.claim_totals': claim_totals,
        'stg.policies': policies,
        'stg.policy_earned_premium': earned_policy
    }

    result = run(SparkSession.builder.master("local[1]").getOrCreate(), tables)
    legacy_rpt_loss_ratio = result['gold.loss_ratio']

    expected_data = [
        ('product1', 2020, 1000, None, None, None),
        ('product2', 2020, 2000, None, None, None),
        ('product3', 2020, 3000, None, None, None)
    ]
    expected_schema = StructType([
        StructField('product_code', StringType()),
        StructField('year', IntegerType()),
        StructField('earned_premium', DecimalType(38, 2)),
        StructField('incurred', DecimalType(38, 3)),
        StructField('claim_count', IntegerType()),
        StructField('loss_ratio', DecimalType(38, 6))
    ])
    expected_df = create_dataframe(expected_data, expected_schema)

    assert legacy_rpt_loss_ratio.collect() == expected_df.collect()

def test_case_2():
    # Test case for NULL handling in comparisons
    claim_totals_data = [
        (1, '2020-01-01', None),
        (2, '2020-01-02', 100),
        (3, '2020-01-03', 200)
    ]
    claim_totals_schema = StructType([
        StructField('policy_id', IntegerType()),
        StructField('loss_date', DateType()),
        StructField('indemnity_paid', DoubleType())
    ])
    claim_totals = create_dataframe(claim_totals_data, claim_totals_schema)

    policies_data = [
        (1, 'policy1'),
        (2, 'policy2'),
        (3, 'policy3')
    ]
    policies_schema = StructType([
        StructField('policy_id', IntegerType()),
        StructField('product_code', StringType())
    ])
    policies = create_dataframe(policies_data, policies_schema)

    earned_policy_data = [
        (1, 'product1', 2020, 1000),
        (2, 'product2', 2020, 2000),
        (3, 'product3', 2020, 3000)
    ]
    earned_policy_schema = StructType([
        StructField('policy_id', IntegerType()),
        StructField('product_code', StringType()),
        StructField('policy_year', IntegerType()),
        StructField('earned_premium', IntegerType())
    ])
    earned_policy = create_dataframe(earned_policy_data, earned_policy_schema)

    tables = {
        'legacy.claim_totals': claim_totals,
        'stg.policies': policies,
        'stg.policy_earned_premium': earned_policy
    }

    result = run(SparkSession.builder.master("local[1]").getOrCreate(), tables)
    legacy_rpt_loss_ratio = result['gold.loss_ratio']

    expected_data = [
        ('product1', 2020, 1000, None, None, None),
        ('product2', 2020, 2000, 100, 1, 0.05),
        ('product3', 2020, 3000, 200, 1, 0.0667)
    ]
    expected_schema = StructType([
        StructField('product_code', StringType()),
        StructField('year', IntegerType()),
        StructField('earned_premium', DecimalType(38, 2)),
        StructField('incurred', DecimalType(38, 3)),
        StructField('claim_count', IntegerType()),
        StructField('loss_ratio', DecimalType(38, 6))
    ])
    expected_df = create_dataframe(expected_data, expected_schema)

    assert legacy_rpt_loss_ratio.collect() == expected_df.collect()

def test_case_3():
    # Test case for COUNT(*) and COUNT(col)
    claim_totals_data = [
        (1, '2020-01-01', 100),
        (2, '2020-01-02', None),
        (3, '2020-01-03', 200)
    ]
    claim_totals_schema = StructType([
        StructField('policy_id', IntegerType()),
        StructField('loss_date', DateType()),
        StructField('indemnity_paid', DoubleType())
    ])
    claim_totals = create_dataframe(claim_totals_data, claim_totals_schema)

    policies_data = [
        (1, 'policy1'),
        (2, 'policy2'),
        (3, 'policy3')
    ]
    policies_schema = StructType([
        StructField('policy_id', IntegerType()),
        StructField('product_code', StringType())
    ])
    policies = create_dataframe(policies_data, policies_schema)

    earned_policy_data = [
        (1, 'product1', 2020, 1000),
        (2, 'product2', 2020, 2000),
        (3, 'product3', 2020, 3000)
    ]
    earned_policy_schema = StructType([
        StructField('policy_id', IntegerType()),
        StructField('product_code', StringType()),
        StructField('policy_year', IntegerType()),
        StructField('earned_premium', IntegerType())
    ])
    earned_policy = create_dataframe(earned_policy_data, earned_policy_schema)

    tables = {
        'legacy.claim_totals': claim_totals,
        'stg.policies': policies,
        'stg.policy_earned_premium': earned_policy
    }

    result = run(SparkSession.builder.master("local[1]").getOrCreate(), tables)
    legacy_rpt_loss_ratio = result['gold.loss_ratio']

    expected_data = [
        ('product1', 2020, 1000, 100, 1, 0.1),
        ('product2', 2020, 2000, None, 0, None),
        ('product3', 2020, 3000, 200, 1, 0.0667)
    ]
    expected_schema = StructType([
        StructField('product_code', StringType()),
        StructField('year', IntegerType()),
        StructField('earned_premium', DecimalType(38, 2)),
        StructField('incurred', DecimalType(38, 3)),
        StructField('claim_count', IntegerType()),
        StructField('loss_ratio', DecimalType(38, 6))
    ])
    expected_df = create_dataframe(expected_data, expected_schema)

    assert legacy_rpt_loss_ratio.collect() == expected_df.collect()

def test_case_4():
    # Test case for ROUND function
    claim_totals_data = [
        (1, '2020-01-01', 100.5),
        (2, '2020-01-02', 200.5),
        (3, '2020-01-03', 300.5)
    ]
    claim_totals_schema = StructType([
        StructField('policy_id', IntegerType()),
        StructField('loss_date', DateType()),
        StructField('indemnity_paid', DoubleType())
    ])
    claim_totals = create_dataframe(claim_totals_data, claim_totals_schema)

    policies_data = [
        (1, 'policy1'),
        (2, 'policy2'),
        (3, 'policy3')
    ]
    policies_schema = StructType([
        StructField('policy_id', IntegerType()),
        StructField('product_code', StringType())
    ])
    policies = create_dataframe(policies_data, policies_schema)

    earned_policy_data = [
        (1, 'product1', 2020, 1000),
        (2, 'product2', 2020, 2000),
        (3, 'product3', 2020, 3000)
    ]
    earned_policy_schema = StructType([
        StructField('policy_id', IntegerType()),
        StructField('product_code', StringType()),
        StructField('policy_year', IntegerType()),
        StructField('earned_premium', IntegerType())
    ])
    earned_policy = create_dataframe(earned_policy_data, earned_policy_schema)

    tables = {
        'legacy.claim_totals': claim_totals,
        'stg.policies': policies,
        'stg.policy_earned_premium': earned_policy
    }

    result = run(SparkSession.builder.master("local[1]").getOrCreate(), tables)
    legacy_rpt_loss_ratio = result['gold.loss_ratio']

    expected_data = [
        ('product1', 2020, 1000, 100.5, 1, 0.1005),
        ('product2', 2020, 2000, 200.5, 1, 0.1003),
        ('product3', 2020, 3000, 300.5, 1, 0.1002)
    ]
    expected_schema = StructType([
        StructField('product_code', StringType()),
        StructField('year', IntegerType()),
        StructField('earned_premium', DecimalType(38, 2)),
        StructField('incurred', DecimalType(38, 3)),
        StructField('claim_count', IntegerType()),
        StructField('loss_ratio', DecimalType(38, 6))
    ])
    expected_df = create_dataframe(expected_data, expected_schema)

    assert legacy_rpt_loss_ratio.collect() == expected_df.collect()

if __name__ == '__main__':
    test_case_1()
    test_case_2()
    test_case_3()
    test_case_4()
