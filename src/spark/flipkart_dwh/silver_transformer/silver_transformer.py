import sys

sys.path.append("/sources/spark_app/")  # noqa
from brand import transform_brand, create_dim_brand_df
from date import transform_date, create_dim_date_df
from fact import create_fact_product_posting
from product import create_dim_product_df
from product_specification import transform_product_specification, create_dim_specification_df
from helper import MinioCredential, create_spark_session, get_products_csv_schema, overwrite_to_table
from product_category import transform_product_category, create_dim_main_category_df, create_dim_category_df, create_dim_sub_category_df


def main(spark_uri: str, hive_uri: str, minio_uri: str, filepath: str, minio_credential: MinioCredential):
    spark = create_spark_session(
        app_name="2lux_ecommerce_data_pipeline", spark_uri=spark_uri, hive_uri=hive_uri, minio_uri=minio_uri, minio_credential=minio_credential
    )
    products_schema = get_products_csv_schema()

    df = spark.read.format("csv").option("escape", '"').option("multiLine", True).option("header", True).schema(products_schema).load(path=filepath)

    db_name = "silver"

    ############################################
    #           DIM_PRODUCT_CATEGORY           #
    ############################################
    modified_category_df = transform_product_category(df=df)
    dim_main_category_df = create_dim_main_category_df(df=modified_category_df)
    dim_category_df = create_dim_category_df(df=modified_category_df)
    dim_sub_category_df = create_dim_sub_category_df(df=modified_category_df)

    overwrite_to_table(df=dim_main_category_df, schema_name=db_name, table_name="dim_main_category")
    overwrite_to_table(df=dim_category_df, schema_name=db_name, table_name="dim_category")
    overwrite_to_table(df=dim_sub_category_df, schema_name=db_name, table_name="dim_sub_category")

    ############################################
    #         DIM_PRODUCT_SPECIFICATION        #
    ############################################
    modified_product_specs_df = transform_product_specification(df=modified_category_df)
    dim_specification_df = create_dim_specification_df(df=modified_product_specs_df)
    overwrite_to_table(df=dim_specification_df, schema_name=db_name, table_name="dim_specification")

    ############################################
    #                DIM_BRAND                 #
    ############################################
    modified_brand_df = transform_brand(df=modified_product_specs_df)
    dim_brand_df = create_dim_brand_df(df=modified_brand_df)
    overwrite_to_table(df=dim_brand_df, schema_name=db_name, table_name="dim_brand")

    ############################################
    #                 DIM_DATE                 #
    ############################################
    modified_date_df = transform_date(df=modified_brand_df)
    dim_date_df = create_dim_date_df(df=modified_date_df)
    overwrite_to_table(df=dim_date_df, schema_name=db_name, table_name="dim_date")

    ############################################
    #                DIM_PRODUCT               #
    ############################################
    dim_product_df = create_dim_product_df(df=modified_date_df)
    overwrite_to_table(df=dim_product_df, schema_name=db_name, table_name="dim_product")

    ############################################
    #          FACT_PRODUCT_POSTING            #
    ############################################
    fact_product_posting_df = create_fact_product_posting(df=modified_date_df)
    overwrite_to_table(df=fact_product_posting_df, schema_name=db_name, table_name="fact_product_posting")


if __name__ == "__main__":
    # TODO: Make it dynamic for local and prod env
    SPARK_URI = f"spark://spark:7077"
    HIVE_URI = f"thrift://hive-metastore:9083"
    MINIO_URI = f"http://minio:9000"
    bronze_container_path = "s3a://bronze"
    FILEPATH = f"{bronze_container_path}/flipkart_ecommerce.csv"
    MINIO_CREDENTIAL = MinioCredential("datalake", "datalake")

    main(spark_uri=SPARK_URI, hive_uri=HIVE_URI, minio_uri=MINIO_URI, filepath=FILEPATH, minio_credential=MINIO_CREDENTIAL)
