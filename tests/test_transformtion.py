import pytest
from pyspark.sql import SparkSession
from transformation import read_orders, transform_orders, order_schema

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()

@pytest.fixture
def sample_df(spark):
    return read_orders(spark, "tests/orders.csv")

""" def test_schema_validation(sample_df):
    print(sample_df.schema)
    print(order_schema)
    assert sample_df.schema == order_schema
 """



def test_column_types(sample_df):
    schema_dict = {f.name: f.dataType.simpleString() for f in sample_df.schema.fields}

    assert schema_dict["order_id"] == "int"
    assert schema_dict["order_date"] == "date"
    assert schema_dict["customer_id"] == "int"
    assert schema_dict["amount"] == "double"
    assert schema_dict["status"] == "string"

def test_filter_valid_records(sample_df):
    result = transform_orders(sample_df)
    assert result.count() == 3

def test_no_null_amount(sample_df):
    result = transform_orders(sample_df)
    assert result.filter("amount IS NULL").count() == 0

def test_no_negative_amount(sample_df):
    result = transform_orders(sample_df)
    assert result.filter("amount < 0").count() == 0

def test_only_completed(sample_df):
    result = transform_orders(sample_df)
    assert result.filter("status != 'COMPLETED'").count() == 0

""" def test_schema_mismatch(spark):
    df = spark.read.csv("tests/orders.csv", header=True, inferSchema=True)
    assert df.schema != order_schema """