from datetime import datetime

import pytest
from pyspark import Row
from pyspark.sql import SparkSession, DataFrame

from src.spark.olist_dwh.silver_transformer.dim_sellers import create_dim_sellers_df
from src.spark.olist_dwh.silver_transformer.dim_customers import create_dim_customers_df
from src.spark.olist_dwh.silver_transformer.dim_order_status import create_dim_order_status_df
from src.spark.olist_dwh.silver_transformer.dim_product_category import create_dim_products_df
from src.spark.olist_dwh.silver_transformer.dim_date import create_dim_date_df
from src.spark.olist_dwh.silver_transformer.fact_orders import create_fact_orders_df
from src.spark.olist_dwh.silver_transformer.fact_payments import create_fact_payments_df
from src.spark.olist_dwh.silver_transformer.fact_reviews import create_fact_reviews_df


@pytest.fixture(scope="module")
def spark_fixture():
    spark = SparkSession.builder.appName("Test Olist DWH").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def dim_date_df_fixture(spark_fixture: SparkSession):
    yield create_dim_date_df(
        spark=spark_fixture,
        min_dates=[datetime.fromisoformat("2017-04-11 12:22:08"), datetime.fromisoformat("2017-04-19 13:25:17")],
        max_dates=[datetime.fromisoformat("2018-06-28 00:00:00"), datetime.fromisoformat("2018-06-13 04:30:33")]
    )


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
        Row(date_id=8589934592, unix_timestamp=1697104260, timestamp=datetime(2023, 10, 12, 17, 51), year="2023", quarter="4", quarter_name="Q4", month="10", month_name="Oct", year_month="202310", year_week="202341", week_day_name="Thu", week_day_type="weekday", timezone="PST", time_of_day="Afternoon"),  # noqa
        Row(date_id=25769803776, unix_timestamp=1697104261, timestamp=datetime(2023, 10, 12, 17, 51, 1), year="2023", quarter="4", quarter_name="Q4", month="10", month_name="Oct", year_month="202310", year_week="202341", week_day_name="Thu", week_day_type="weekday", timezone="PST", time_of_day="Afternoon"),  # noqa
        Row(date_id=42949672960, unix_timestamp=1697104262, timestamp=datetime(2023, 10, 12, 17, 51, 2), year="2023", quarter="4", quarter_name="Q4", month="10", month_name="Oct", year_month="202310", year_week="202341", week_day_name="Thu", week_day_type="weekday", timezone="PST", time_of_day="Afternoon"),  # noqa
        Row(date_id=60129542144, unix_timestamp=1697104263, timestamp=datetime(2023, 10, 12, 17, 51, 3), year="2023", quarter="4", quarter_name="Q4", month="10", month_name="Oct", year_month="202310", year_week="202341", week_day_name="Thu", week_day_type="weekday", timezone="PST", time_of_day="Afternoon"),  # noqa
        Row(date_id=77309411328, unix_timestamp=1697104264, timestamp=datetime(2023, 10, 12, 17, 51, 4), year="2023", quarter="4", quarter_name="Q4", month="10", month_name="Oct", year_month="202310", year_week="202341", week_day_name="Thu", week_day_type="weekday", timezone="PST", time_of_day="Afternoon"),  # noqa
        Row(date_id=94489280512, unix_timestamp=1697104265, timestamp=datetime(2023, 10, 12, 17, 51, 5), year="2023", quarter="4", quarter_name="Q4", month="10", month_name="Oct", year_month="202310", year_week="202341", week_day_name="Thu", week_day_type="weekday", timezone="PST", time_of_day="Afternoon")  # noqa
    ]
    assert dim_date_df.count() == 6
    assert dim_date_df.collect() == expected_dim_date


def test_fact_orders_creation(spark_fixture: SparkSession, dim_date_df_fixture: DataFrame):
    orders_data = [
        {
            "order_id": "8c2b13adf3f377c8f2b06b04321b0925",
            "customer_id": "0aad2e31b3c119c26acb8a47768cd00a",
            "order_status": "delivered",
            "order_purchase_timestamp": "2017-11-17 19:46:08",
            "order_approved_at": "2017-11-17 21:31:03",
            "order_delivered_carrier_date": "2017-11-21 12:57:04",
            "order_delivered_customer_date": "2017-11-29 20:13:45",
            "order_estimated_delivery_date": "2017-12-20 00:00:00"
        },
        {
            "order_id": "136cce7faa42fdb2cefd53fdc79a6098",
            "customer_id": "ed0271e0b7da060a393796590e7b737a",
            "order_status": "invoiced",
            "order_purchase_timestamp": "2017-04-11 12:22:08",
            "order_approved_at": "2017-04-13 13:25:17",
            "order_delivered_carrier_date": "2017-05-08 00:00:00",
            "order_delivered_customer_date": "2017-05-09 00:00:00",
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
    order_items_data = [
        {
            "order_id": "8c2b13adf3f377c8f2b06b04321b0925",
            "order_item_id": 1,
            "product_id": "6f59fe49d85eb1353b826d6b5a55e753",
            "seller_id": "977f9f63dd360c2a32ece2f93ad6d306",
            "shipping_limit_date": "2017-11-23 20:31:40",
            "price": 90.90,
            "freight_value": 21.08
        },
        {
            "order_id": "8c2b13adf3f377c8f2b06b04321b0925",
            "order_item_id": 2,
            "product_id": "5c818ca21204caf8ce1599617751ff49",
            "seller_id": "54965bbe3e4f07ae045b90b0b8541f52",
            "shipping_limit_date": "2017-11-23 20:31:40",
            "price": 160.00,
            "freight_value": 21.08
        },
        {
            "order_id": "8c2b13adf3f377c8f2b06b04321b0925",
            "order_item_id": 3,
            "product_id": "b75ad41bddb7dc94c7e555d9f78f5e8a",
            "seller_id": "1dfe5347016252a7884b694d4f10f5c4",
            "shipping_limit_date": "2017-11-23 20:31:40",
            "price": 61.00,
            "freight_value": 21.08
        },
        {
            "order_id": "8c2b13adf3f377c8f2b06b04321b0925",
            "order_item_id": 4,
            "product_id": "601a360bd2a916ecef0e88de72a6531a",
            "seller_id": "7a67c85e85bb2ce8582c35f2203ad736",
            "shipping_limit_date": "2017-11-23 20:31:40",
            "price": 129.99,
            "freight_value": 42.16
        },
        {
            "order_id": "136cce7faa42fdb2cefd53fdc79a6098",
            "order_item_id": 1,
            "product_id": "a1804276d9941ac0733cfd409f5206eb",
            "seller_id": "dc8798cbf453b7e0f98745e396cc5616",
            "shipping_limit_date": "2017-04-19 13:25:17",
            "price": 49.90,
            "freight_value": 16.05
        },
        {
            "order_id": "ee64d42b8cf066f35eac1cf57de1aa85",
            "order_item_id": 1,
            "product_id": "c50ca07e9e4db9ea5011f06802c0aea0",
            "seller_id": "e9779976487b77c6d4ac45f75ec7afe9",
            "shipping_limit_date": "2018-06-13 04:30:33",
            "price": 14.49,
            "freight_value": 7.87
        }
    ]
    order_status_data = [
        {
            "order_status_id": 1,
            "order_status": "shipped",
        },
        {
            "order_status_id": 2,
            "order_status": "invoiced",
        },
        {
            "order_status_id": 3,
            "order_status": "delivered",
        }
    ]

    orders_df = spark_fixture.createDataFrame(data=orders_data)
    order_items_df = spark_fixture.createDataFrame(data=order_items_data)
    dim_order_status_df = spark_fixture.createDataFrame(data=order_status_data)

    fact_orders_df = create_fact_orders_df(
        spark=spark_fixture,
        orders_df=orders_df,
        order_items_df=order_items_df,
        dim_order_status_df=dim_order_status_df,
        dim_date_df=dim_date_df_fixture
    )

    expected_fact_orders = [
        Row(order_id="136cce7faa42fdb2cefd53fdc79a6098", order_item_id=1, product_id="a1804276d9941ac0733cfd409f5206eb", seller_id="dc8798cbf453b7e0f98745e396cc5616", customer_id="ed0271e0b7da060a393796590e7b737a", order_status_id=2, shipping_limit_date_id=694989, order_purchase_timestamp_id=0, order_approved_at_id=176589, order_delivered_carrier_date_id=2288272, order_delivered_customer_date_id=2374672, order_estimated_delivery_date_id=2374672, price=49.9, freight_value=16.05),  # noqa
        Row(order_id="8c2b13adf3f377c8f2b06b04321b0925", order_item_id=4, product_id="601a360bd2a916ecef0e88de72a6531a", seller_id="7a67c85e85bb2ce8582c35f2203ad736", customer_id="0aad2e31b3c119c26acb8a47768cd00a", order_status_id=3, shipping_limit_date_id=51540047988, order_purchase_timestamp_id=42952778153, order_approved_at_id=42952784448, order_delivered_carrier_date_id=51539847912, order_delivered_customer_date_id=51540565313, order_estimated_delivery_date_id=51542306888, price=129.99, freight_value=42.16),  # noqa
        Row(order_id="8c2b13adf3f377c8f2b06b04321b0925", order_item_id=3, product_id="b75ad41bddb7dc94c7e555d9f78f5e8a", seller_id="1dfe5347016252a7884b694d4f10f5c4", customer_id="0aad2e31b3c119c26acb8a47768cd00a", order_status_id=3, shipping_limit_date_id=51540047988, order_purchase_timestamp_id=42952778153, order_approved_at_id=42952784448, order_delivered_carrier_date_id=51539847912, order_delivered_customer_date_id=51540565313, order_estimated_delivery_date_id=51542306888, price=61.0, freight_value=21.08),  # noqa
        Row(order_id="8c2b13adf3f377c8f2b06b04321b0925", order_item_id=2, product_id="5c818ca21204caf8ce1599617751ff49", seller_id="54965bbe3e4f07ae045b90b0b8541f52", customer_id="0aad2e31b3c119c26acb8a47768cd00a", order_status_id=3, shipping_limit_date_id=51540047988, order_purchase_timestamp_id=42952778153, order_approved_at_id=42952784448, order_delivered_carrier_date_id=51539847912, order_delivered_customer_date_id=51540565313, order_estimated_delivery_date_id=51542306888, price=160.0, freight_value=21.08),  # noqa
        Row(order_id="8c2b13adf3f377c8f2b06b04321b0925", order_item_id=1, product_id="6f59fe49d85eb1353b826d6b5a55e753", seller_id="977f9f63dd360c2a32ece2f93ad6d306", customer_id="0aad2e31b3c119c26acb8a47768cd00a", order_status_id=3, shipping_limit_date_id=51540047988, order_purchase_timestamp_id=42952778153, order_approved_at_id=42952784448, order_delivered_carrier_date_id=51539847912, order_delivered_customer_date_id=51540565313, order_estimated_delivery_date_id=51542306888, price=90.9, freight_value=21.08),  # noqa
        Row(order_id="ee64d42b8cf066f35eac1cf57de1aa85", order_item_id=1, product_id="c50ca07e9e4db9ea5011f06802c0aea0", seller_id="e9779976487b77c6d4ac45f75ec7afe9", customer_id="caded193e8e47b8362864762a83db3c5", order_status_id=1, shipping_limit_date_id=94491186634, order_purchase_timestamp_id=94490453089, order_approved_at_id=94490495479, order_delivered_carrier_date_id=94490531521, order_delivered_customer_date_id=94490531521, order_estimated_delivery_date_id=94492466401, price=14.49, freight_value=7.87)  # noqa
    ]

    assert fact_orders_df.count() == 6
    assert fact_orders_df.collect() == expected_fact_orders


def test_fact_payments_creation(spark_fixture: SparkSession, dim_date_df_fixture: DataFrame):
    payments_data = [
        {
            "order_id": "8c2b13adf3f377c8f2b06b04321b0925",
            "payment_sequential": 1,
            "payment_type": "credit_card",
            "payment_installments": 5,
            "payment_value": 547.29
        },
        {
            "order_id": "136cce7faa42fdb2cefd53fdc79a6098",
            "payment_sequential": 1,
            "payment_type": "credit_card",
            "payment_installments": 1,
            "payment_value": 65.95
        },
        {
            "order_id": "ac3b0c224349e4ca9a0b0f2e8fbc4c75",
            "payment_sequential": 1,
            "payment_type": "credit_card",
            "payment_installments": 1,
            "payment_value": 2.29
        },
        {
            "order_id": "ac3b0c224349e4ca9a0b0f2e8fbc4c75",
            "payment_sequential": 2,
            "payment_type": "voucher",
            "payment_installments": 1,
            "payment_value": 20.00
        },
        {
            "order_id": "ac3b0c224349e4ca9a0b0f2e8fbc4c75",
            "payment_sequential": 3,
            "payment_type": "voucher",
            "payment_installments": 1,
            "payment_value": 20.00
        }
    ]
    orders_data = [
        {
            "order_id": "8c2b13adf3f377c8f2b06b04321b0925",
            "customer_id": "0aad2e31b3c119c26acb8a47768cd00a",
            "order_status": "delivered",
            "order_purchase_timestamp": "2017-11-17 19:46:08",
            "order_approved_at": "2017-11-17 21:31:03",
            "order_delivered_carrier_date": "2017-11-21 12:57:04",
            "order_delivered_customer_date": "2017-11-29 20:13:45",
            "order_estimated_delivery_date": "2017-12-20 00:00:00"
        },
        {
            "order_id": "136cce7faa42fdb2cefd53fdc79a6098",
            "customer_id": "ed0271e0b7da060a393796590e7b737a",
            "order_status": "invoiced",
            "order_purchase_timestamp": "2017-04-11 12:22:08",
            "order_approved_at": "2017-04-13 13:25:17",
            "order_delivered_carrier_date": "2017-05-08 00:00:00",
            "order_delivered_customer_date": "2017-05-09 00:00:00",
            "order_estimated_delivery_date": "2017-05-09 00:00:00"
        },
        {
            "order_id": "ac3b0c224349e4ca9a0b0f2e8fbc4c75",
            "customer_id": "f444bb4bffe058f24c3b5b5a0c0f46b6",
            "order_status": "delivered",
            "order_purchase_timestamp": "2018-05-16 04:47:08",
            "order_approved_at": "2018-05-16 04:55:11",
            "order_delivered_carrier_date": "2018-05-16 14:15:00",
            "order_delivered_customer_date": "2018-05-17 15:06:54",
            "order_estimated_delivery_date": "2018-05-28 00:00:00"
        }
    ]
    order_status_data = [
        {
            "order_status_id": 1,
            "order_status": "shipped",
        },
        {
            "order_status_id": 2,
            "order_status": "invoiced",
        },
        {
            "order_status_id": 3,
            "order_status": "delivered",
        }
    ]
    payments_df = spark_fixture.createDataFrame(data=payments_data)
    orders_df = spark_fixture.createDataFrame(data=orders_data)
    dim_order_status_df = spark_fixture.createDataFrame(data=order_status_data)
    fact_payments_df = create_fact_payments_df(
        spark=spark_fixture, payments_df=payments_df, orders_df=orders_df, dim_date_df=dim_date_df_fixture, dim_order_status_df=dim_order_status_df
    )
    expected_fact_payments = [
        Row(order_id="136cce7faa42fdb2cefd53fdc79a6098", customer_id="ed0271e0b7da060a393796590e7b737a", order_status_id=2, order_purchase_timestamp_id=0, order_approved_at_id=176589, order_delivered_carrier_date_id=2288272, order_delivered_customer_date_id=2374672, order_estimated_delivery_date_id=2374672, payment_sequential=1, payment_type="credit_card", payment_installments=1, payment_value=65.95),  # noqa
        Row(order_id="8c2b13adf3f377c8f2b06b04321b0925", customer_id="0aad2e31b3c119c26acb8a47768cd00a", order_status_id=3, order_purchase_timestamp_id=42952778153, order_approved_at_id=42952784448, order_delivered_carrier_date_id=51539847912, order_delivered_customer_date_id=51540565313, order_estimated_delivery_date_id=51542306888, payment_sequential=1, payment_type="credit_card", payment_installments=5, payment_value=547.29),  # noqa
        Row(order_id="ac3b0c224349e4ca9a0b0f2e8fbc4c75", customer_id="f444bb4bffe058f24c3b5b5a0c0f46b6", order_status_id=3, order_purchase_timestamp_id=85902019726, order_approved_at_id=85902020209, order_delivered_carrier_date_id=85902053798, order_delivered_customer_date_id=85902143312, order_estimated_delivery_date_id=94489788001, payment_sequential=1, payment_type="credit_card", payment_installments=1, payment_value=2.29),  # noqa
        Row(order_id="ac3b0c224349e4ca9a0b0f2e8fbc4c75", customer_id="f444bb4bffe058f24c3b5b5a0c0f46b6", order_status_id=3, order_purchase_timestamp_id=85902019726, order_approved_at_id=85902020209, order_delivered_carrier_date_id=85902053798, order_delivered_customer_date_id=85902143312, order_estimated_delivery_date_id=94489788001, payment_sequential=2, payment_type="voucher", payment_installments=1, payment_value=20.0),  # noqa
        Row(order_id="ac3b0c224349e4ca9a0b0f2e8fbc4c75", customer_id="f444bb4bffe058f24c3b5b5a0c0f46b6", order_status_id=3, order_purchase_timestamp_id=85902019726, order_approved_at_id=85902020209, order_delivered_carrier_date_id=85902053798, order_delivered_customer_date_id=85902143312, order_estimated_delivery_date_id=94489788001, payment_sequential=3, payment_type="voucher", payment_installments=1, payment_value=20.0)  # noqa
    ]

    assert fact_payments_df.collect() == expected_fact_payments


def test_fact_reviews_creation(spark_fixture: SparkSession, dim_date_df_fixture: DataFrame):
    reviews_data = [
        {
            "review_id": "7c92e0cf5216a579027044df83dccb6f",
            "order_id": "8c2b13adf3f377c8f2b06b04321b0925",
            "review_score": 1,
            "review_comment_title": "",
            "review_comment_message": "Não recebi meus produtos ",
            "review_creation_date": "2017-11-30 00:00:00",
            "review_answer_timestamp": "2017-12-04 18:55:07"
        },
        {
            "review_id": "e07549ef5311abcc92ba1784b093fb56",
            "order_id": "136cce7faa42fdb2cefd53fdc79a6098",
            "review_score": 2,
            "review_comment_title": "",
            "review_comment_message": "fiquei triste por n ter me atendido.",
            "review_creation_date": "2017-05-13 00:00:00",
            "review_answer_timestamp": "2017-05-13 20:25:42"
        },
        {
            "review_id": "148168e0fafc52f1f67e8e9abccacf49",
            "order_id": "ac3b0c224349e4ca9a0b0f2e8fbc4c75",
            "review_score": 4,
            "review_comment_title": "",
            "review_comment_message": "",
            "review_creation_date": "2018-05-18 00:00:00",
            "review_answer_timestamp": "2018-05-20 19:42:14"
        }
    ]
    orders_data = [
        {
            "order_id": "8c2b13adf3f377c8f2b06b04321b0925",
            "customer_id": "0aad2e31b3c119c26acb8a47768cd00a",
            "order_status": "delivered",
            "order_purchase_timestamp": "2017-11-17 19:46:08",
            "order_approved_at": "2017-11-17 21:31:03",
            "order_delivered_carrier_date": "2017-11-21 12:57:04",
            "order_delivered_customer_date": "2017-11-29 20:13:45",
            "order_estimated_delivery_date": "2017-12-20 00:00:00"
        },
        {
            "order_id": "136cce7faa42fdb2cefd53fdc79a6098",
            "customer_id": "ed0271e0b7da060a393796590e7b737a",
            "order_status": "invoiced",
            "order_purchase_timestamp": "2017-04-11 12:22:08",
            "order_approved_at": "2017-04-13 13:25:17",
            "order_delivered_carrier_date": "2017-05-08 00:00:00",
            "order_delivered_customer_date": "2017-05-09 00:00:00",
            "order_estimated_delivery_date": "2017-05-09 00:00:00"
        },
        {
            "order_id": "ac3b0c224349e4ca9a0b0f2e8fbc4c75",
            "customer_id": "f444bb4bffe058f24c3b5b5a0c0f46b6",
            "order_status": "delivered",
            "order_purchase_timestamp": "2018-05-16 04:47:08",
            "order_approved_at": "2018-05-16 04:55:11",
            "order_delivered_carrier_date": "2018-05-16 14:15:00",
            "order_delivered_customer_date": "2018-05-17 15:06:54",
            "order_estimated_delivery_date": "2018-05-28 00:00:00"
        }
    ]
    order_status_data = [
        {
            "order_status_id": 1,
            "order_status": "shipped",
        },
        {
            "order_status_id": 2,
            "order_status": "invoiced",
        },
        {
            "order_status_id": 3,
            "order_status": "delivered",
        }
    ]
    reviews_df = spark_fixture.createDataFrame(data=reviews_data)
    orders_df = spark_fixture.createDataFrame(data=orders_data)
    dim_order_status_df = spark_fixture.createDataFrame(data=order_status_data)
    fact_reviews_df = create_fact_reviews_df(
        reviews_df=reviews_df, dim_date_df=dim_date_df_fixture, orders_df=orders_df, dim_order_status_df=dim_order_status_df
    )
    expected_fact_reviews = [
        Row(review_id="148168e0fafc52f1f67e8e9abccacf49", order_id="ac3b0c224349e4ca9a0b0f2e8fbc4c75", customer_id="f444bb4bffe058f24c3b5b5a0c0f46b6", order_status_id=3, review_creation_date_id=85902175298, review_answer_timestamp_id=85902419032, review_score=4, review_comment_title="", review_comment_message=""),
        Row(review_id="e07549ef5311abcc92ba1784b093fb56", order_id="136cce7faa42fdb2cefd53fdc79a6098", customer_id="ed0271e0b7da060a393796590e7b737a", order_status_id=2, review_creation_date_id=2720272, review_answer_timestamp_id=2793814, review_score=2, review_comment_title="", review_comment_message="fiquei triste por n ter me atendido."),
        Row(review_id="7c92e0cf5216a579027044df83dccb6f", order_id="8c2b13adf3f377c8f2b06b04321b0925", customer_id="0aad2e31b3c119c26acb8a47768cd00a", order_status_id=3, review_creation_date_id=51540578888, review_answer_timestamp_id=51540992595, review_score=1, review_comment_title="", review_comment_message="Não recebi meus produtos ")
    ]

    assert fact_reviews_df.count() == 3
    assert fact_reviews_df.collect() == expected_fact_reviews
