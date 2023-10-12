from datetime import datetime

import pytest
from pyspark import Row
from pyspark.sql import SparkSession

from src.spark.olist_dwh.silver_transformer.dim_sellers import create_dim_sellers_df
from src.spark.olist_dwh.silver_transformer.dim_customers import create_dim_customers_df
from src.spark.olist_dwh.silver_transformer.dim_order_status import create_dim_order_status_df
from src.spark.olist_dwh.silver_transformer.dim_product_category import create_dim_products_df
from src.spark.olist_dwh.silver_transformer.dim_date import create_dim_date_df


@pytest.fixture(scope="module")
def spark_fixture():
    spark = SparkSession.builder.appName("Test Olist DWH").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


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


def test_dim_order_status_creation(spark_fixture: SparkSession):
    orders_data = [
        {
            "order_id": "53cdb2fc8bc7dce0b6741e2150273451",
            "customer_id": "b0830fb4747a6c6d20dea0b8c802d7ef",
            "order_status": "delivered",
            "order_purchaser_timestamp": "2018-07-24 20:41:37",
            "order_approved_at": "2018-07-26 03:24:27",
            "order_delivered_carrier_date": "2018-07-26 14:31:00",
            "order_delivered_customer_date": "2018-08-07 15:27:45",
            "order_estimated_delivery_date": "2018-08-13 00:00:00"
        },
        {
            "order_id": "136cce7faa42fdb2cefd53fdc79a6098",
            "customer_id": "ed0271e0b7da060a393796590e7b737a",
            "order_status": "invoiced",
            "order_purchaser_timestamp": "2017-04-11 12:22:08",
            "order_approved_at": "2017-04-13 13:25:17",
            "order_delivered_carrier_date": None,
            "order_delivered_customer_date": None,
            "order_estimated_delivery_date": "2017-05-09 00:00:00"
        },
        {
            "order_id": "ee64d42b8cf066f35eac1cf57de1aa85",
            "customer_id": "caded193e8e47b8362864762a83db3c5",
            "order_status": "shipped",
            "order_purchase_timestamp": "2018-06-04 16:44:48",
            "order_approved_at": "2018-06-05 04:31:18",
            "order_delivered_carrier_date": "2018-06-05 14:32:00",
            "order_delivered_customer_date": "2018-06-05 14:32:00",
            "order_estimated_delivery_date": "2018-06-28 00:00:00"
        }
    ]
    orders_df = spark_fixture.createDataFrame(data=orders_data)
    dim_order_status = create_dim_order_status_df(orders_df=orders_df)
    expected_order_status = [
        Row(order_status="shipped", order_status_id=1),
        Row(order_status="invoiced", order_status_id=2),
        Row(order_status="delivered", order_status_id=3)
    ]

    assert dim_order_status.count() == 3
    assert dim_order_status.collect() == expected_order_status


def test_dim_product_category(spark_fixture: SparkSession):
    products_data = [
        {
            "product_id": "1e9e8ef04dbcff4541ed26657ea517e5",
            "product_category_name": "perfumaria",
            "product_name_lenght": 40,
            "product_description_lenght": 287,
            "product_photos_qty": 1,
            "product_weight_g": 225,
            "product_length_cm": 16,
            "product_height_cm": 10,
            "product_widht_cm": 14
        },
        {
            "product_id": "3fcd8dfe610c62edfb51de2630cd9ef4",
            "product_category_name": "bebes",
            "product_name_lenght": 50,
            "product_description_lenght": 509,
            "product_photos_qty": 4,
            "product_weight_g": 5700,
            "product_length_cm": 41,
            "product_height_cm": 17,
            "product_widht_cm": 49
        },
        {
            "product_id": "22937a73f92a33040ab4e2540355a5d8",
            "product_category_name": "fashion_bolsas_e_acessorios",
            "product_name_lenght": 50,
            "product_description_lenght": 554,
            "product_photos_qty": 3,
            "product_weight_g": 100,
            "product_length_cm": 16,
            "product_height_cm": 5,
            "product_widht_cm": 11
        },
    ]
    product_category_name_translation = [
        {
            "product_category_name": "perfumaria",
            "product_category_name_english": "perfumery",
        },
        {
            "product_category_name": "bebes",
            "product_category_name_english": "baby",
        },
        {
            "product_category_name": "fashion_bolsas_e_acessorios",
            "product_category_name_english": "fashion_bags_accessories",
        },
    ]
    products_df = spark_fixture.createDataFrame(data=products_data)
    products_translation_df = spark_fixture.createDataFrame(data=product_category_name_translation)
    dim_products_df = create_dim_products_df(products_df=products_df, product_category_name_translation_df=products_translation_df)
    expected_dim_products = [
        Row(product_id="1e9e8ef04dbcff4541ed26657ea517e5", product_category_name="perfumaria", product_category_name_english="perfumery"),
        Row(product_id="3fcd8dfe610c62edfb51de2630cd9ef4", product_category_name="bebes", product_category_name_english="baby"),
        Row(product_id="22937a73f92a33040ab4e2540355a5d8", product_category_name="fashion_bolsas_e_acessorios", product_category_name_english="fashion_bags_accessories"),  # noqa
    ]

    assert dim_products_df.count() == 3
    assert dim_products_df.collect() == expected_dim_products


def test_dim_date_creation(spark_fixture: SparkSession):
    dim_date_df = create_dim_date_df(
        spark=spark_fixture,
        min_dates=[datetime(2023, 10, 12, 17, 51, 0), datetime(2023, 10, 12, 17, 51, 30), datetime(2023, 10, 17, 17, 51, 0)],
        max_dates=[datetime(2023, 10, 12, 17, 51, 5), datetime(2023, 10, 12, 17, 1, 9), datetime(2023, 10, 12, 17, 2, 35)]
    )
    expected_dim_date = [
        Row(date_id=1, unix_timestamp=1697104260, timestamp=datetime(2023, 10, 12, 17, 51), year='2023', quarter='4', quarter_name='04', month='10', month_name='Oct', year_month='202310', year_week='202341', week_day_name='Thu', week_day_type='weekday', timezone='PST', time_of_day='Afternoon'),  # noqa
        Row(date_id=2, unix_timestamp=1697104261, timestamp=datetime(2023, 10, 12, 17, 51, 1), year='2023', quarter='4', quarter_name='04', month='10', month_name='Oct', year_month='202310', year_week='202341', week_day_name='Thu', week_day_type='weekday', timezone='PST', time_of_day='Afternoon'),  # noqa
        Row(date_id=3, unix_timestamp=1697104262, timestamp=datetime(2023, 10, 12, 17, 51, 2), year='2023', quarter='4', quarter_name='04', month='10', month_name='Oct', year_month='202310', year_week='202341', week_day_name='Thu', week_day_type='weekday', timezone='PST', time_of_day='Afternoon'),  # noqa
        Row(date_id=4, unix_timestamp=1697104263, timestamp=datetime(2023, 10, 12, 17, 51, 3), year='2023', quarter='4', quarter_name='04', month='10', month_name='Oct', year_month='202310', year_week='202341', week_day_name='Thu', week_day_type='weekday', timezone='PST', time_of_day='Afternoon'),  # noqa
        Row(date_id=5, unix_timestamp=1697104264, timestamp=datetime(2023, 10, 12, 17, 51, 4), year='2023', quarter='4', quarter_name='04', month='10', month_name='Oct', year_month='202310', year_week='202341', week_day_name='Thu', week_day_type='weekday', timezone='PST', time_of_day='Afternoon'),  # noqa
        Row(date_id=6, unix_timestamp=1697104265, timestamp=datetime(2023, 10, 12, 17, 51, 5), year='2023', quarter='4', quarter_name='04', month='10', month_name='Oct', year_month='202310', year_week='202341', week_day_name='Thu', week_day_type='weekday', timezone='PST', time_of_day='Afternoon')  # noqa
    ]
    assert dim_date_df.count() == 6
    assert dim_date_df.collect() == expected_dim_date
   
