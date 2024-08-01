import pytest
from lib.DataReader import read_customers
from lib.DataReader import read_orders
from lib.DataManipulation import filter_closed_orders
from lib.ConfigReader import get_app_config

# @pytest.fixture is help you in fix the same object thats comes often
# @pytest.fixture
# def spark():
#     return get_spark_session("LOCAL")

# You may create a seprate file and put the fixture there conftest.py

# def test_read_customers_df():
#     spark = read_customers(spark,"LOCAL")
#     read_customers(spark,"LOCAL")
#     customer_count = read_customers(spark,"LOCAL").count()
#     assert customer_count == 12433

# def test_read_orders_df():
#     spark = read_customers(spark,"LOCAL")
#     orders_count = read_orders(spark,"LOCAL").count()
#     assert orders_count == 68881

# def test_filter_closed_orders():
#     spark = read_customers(spark,"LOCAL")
#     orders_df = read_orders(spark,"LOCAL")
#     filtered_count = filter_closed_orders(orders_df).count()
#     assert filtered_count == 7556


def test_read_customers_df(spark):
    customer_count = read_customers(spark,"LOCAL").count()
    assert customer_count == 12433

def test_read_orders_df(spark):
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68881

def test_filter_closed_orders(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 7556

def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders_raw.csv"

