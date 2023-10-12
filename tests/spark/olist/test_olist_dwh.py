import pytest
from pyspark import Row
from pyspark.sql import SparkSession

from src.spark.olist_dwh.silver_transformer.dim_sellers import create_dim_sellers_df
from src.spark.olist_dwh.silver_transformer.dim_customers import create_dim_customers_df


@pytest.fixture
def spark_fixture():
    return SparkSession.builder.appName("Test Olist DWH").master("local[*]").getOrCreate()


def test_dim_customers_creation(spark_fixture: SparkSession):
    customers_data = [
        {
            "customer_id": "06b8999e2fba1a1fbc88172c00ba8bc7",
            "customer_unique_id": "861eff4711a542e4b93843c6dd7febb0",
            "customer_zip_code_prefix": "14409",
            "customer_city": "franca",
            "customer_state": "SP"
        },
        {
            "customer_id": "4e7b3e00288586ebd08712fdd0374a03",
            "customer_unique_id": "060e732b5b29e8181a18229c7b0b2b5e",
            "customer_zip_code_prefix": "01151",
            "customer_city": "sao paulo",
            "customer_state": "SP"
        },
        {
            "customer_id": "5e274e7a0c3809e14aba7ad5aae0d407",
            "customer_unique_id": "57b2a98a409812fe9618067b6b8ebe4f",
            "customer_zip_code_prefix": "35182",
            "customer_city": "timoteo",
            "customer_state": "MG"
        },
        {
            "customer_id": "5adf08e34b2e993982a47070956c5c65",
            "customer_unique_id": "1175e95fb47ddff9de6b2b06188f7e0d",
            "customer_zip_code_prefix": "81560",
            "customer_city": "curitiba",
            "customer_state": "PR"
        }
    ]
    geolocations_data = [
        {
            "geolocation_zip_code_prefix": "14409",
            "geolocation_lat": -20.509897499999994,
            "geolocation_lng": -47.3978655,
            "geolocation_city": "franca",
            "geolocation_state": "SP"
        },
        {
            "geolocation_zip_code_prefix": "14409",
            "geolocation_lat": -20.497396193014072,
            "geolocation_lng": -47.39924094190359,
            "geolocation_city": "franca",
            "geolocation_state": "SP"
        },
    ]

    customers_df = spark_fixture.createDataFrame(data=customers_data)
    geolocations_df = spark_fixture.createDataFrame(data=geolocations_data)

    dim_customer_df = create_dim_customers_df(customers_df=customers_df, geolocations_df=geolocations_df)

    expected_dim_customers = [
        Row(
            customer_id="4e7b3e00288586ebd08712fdd0374a03",
            customer_unique_id="060e732b5b29e8181a18229c7b0b2b5e",
            customer_zip_code_prefix="01151",
            customer_city="sao paulo",
            customer_state="SP",
            customer_location_latitude=None,
            customer_location_longitude=None
        ),
        Row(
            customer_id="06b8999e2fba1a1fbc88172c00ba8bc7",
            customer_unique_id="861eff4711a542e4b93843c6dd7febb0",
            customer_zip_code_prefix="14409",
            customer_city="franca",
            customer_state="SP",
            customer_location_latitude=-20.497396193014072,
            customer_location_longitude=-47.39924094190359
        ),
        Row(
            customer_id="5e274e7a0c3809e14aba7ad5aae0d407",
            customer_unique_id="57b2a98a409812fe9618067b6b8ebe4f",
            customer_zip_code_prefix="35182",
            customer_city="timoteo",
            customer_state="MG",
            customer_location_latitude=None,
            customer_location_longitude=None
        ),
        Row(
            customer_id="5adf08e34b2e993982a47070956c5c65",
            customer_unique_id="1175e95fb47ddff9de6b2b06188f7e0d",
            customer_zip_code_prefix="81560",
            customer_city="curitiba",
            customer_state="PR",
            customer_location_latitude=None,
            customer_location_longitude=None
        )
    ]

    assert dim_customer_df.count() == 4
    assert dim_customer_df.where("customer_location_latitude IS NOT NULL AND customer_location_longitude IS NOT NULL").count() == 1
    assert dim_customer_df.collect() == expected_dim_customers


def test_dim_sellers_creation(spark_fixture: SparkSession):
    sellers_data = [
        {
            "seller_id": "3442f8959a84dea7ee197c632cb2df15",
            "seller_zip_code_prefix": "13023",
            "seller_city": "campinas",
            "seller_state": "SP"
        },
        {
            "seller_id": "e49c26c3edfa46d227d5121a6b6e4d37,,",
            "seller_zip_code_prefix": "55325",
            "seller_city": "brejao",
            "seller_state": "PE"
        },
        {
            "seller_id": "ce3ad9de960102d0677a81f5d0bb7b2d",
            "seller_zip_code_prefix": "20031",
            "seller_city": "rio de janeiro",
            "seller_state": "RJ"
        }
    ]
    geolocations_data = [
        {
            "geolocation_zip_code_prefix": "13023",
            "geolocation_lat": -22.898536428530225,
            "geolocation_lng": -47.063125168330544,
            "geolocation_city": "campinas",
            "geolocation_state": "SP"
        },
        {
            "geolocation_zip_code_prefix": "13023",
            "geolocation_lat": -22.895499290034056,
            "geolocation_lng": -47.061943920624365,
            "geolocation_city": "campinas",
            "geolocation_state": "SP"
        },
        {
            "geolocation_zip_code_prefix": "20031",
            "geolocation_lat": -22.90786339246341,
            "geolocation_lng": -43.17569296488547,
            "geolocation_city": "rio de janeiro",
            "geolocation_state": "RJ"
        },
    ]

    sellers_df = spark_fixture.createDataFrame(data=sellers_data)
    geolocations_df = spark_fixture.createDataFrame(data=geolocations_data)

    dim_seller_df = create_dim_sellers_df(sellers_df=sellers_df, geolocations_df=geolocations_df)

    expected_dim_customers = [Row(seller_id="3442f8959a84dea7ee197c632cb2df15", seller_zip_code_prefix="13023", seller_city="campinas", seller_state="SP", seller_location_latitude=-22.895499290034056, seller_location_longitude=-47.061943920624365), Row(seller_id="ce3ad9de960102d0677a81f5d0bb7b2d", seller_zip_code_prefix="20031", seller_city="rio de janeiro", seller_state="RJ", seller_location_latitude=-22.90786339246341, seller_location_longitude=-43.17569296488547), Row(seller_id="e49c26c3edfa46d227d5121a6b6e4d37,,", seller_zip_code_prefix="55325", seller_city="brejao", seller_state="PE", seller_location_latitude=None, seller_location_longitude=None)]

    assert dim_seller_df.count() == 3
    assert dim_seller_df.where("seller_location_latitude IS NOT NULL AND seller_location_longitude IS NOT NULL").count() == 2
    assert dim_seller_df.collect() == expected_dim_customers
