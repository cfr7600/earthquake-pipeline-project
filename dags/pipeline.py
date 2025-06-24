from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import os
from include.data.usgs_data import fetchData

SNOWFLAKE_RAW_DATA_TABLE = "raw_data"
SNOWFLAKE_HEATMAP_TABLE = "heatmap"
SNOWFLAKE_COUNTRIES_TABLE = "country_stats"
SNOWFLAKE_CONN_ID = "snowflake_default"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    'earthquake_pipeline',
    default_args=default_args,
    description='End to end pipeline: api -> s3 -> snowflake',
    schedule='@daily',
    template_searchpath='/usr/local/airflow/include',
    start_date=datetime(2025, 6, 20),
    catchup=False,
) as dag:
    
    fetch_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetchData,
    )
    
    upload_task = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename="/usr/local/airflow/include/data/earthquake-10-year-data.jsonl",
        dest_key="earthquake-10-year-data.jsonl",
        dest_bucket= os.getenv("BUCKET_NAME"),
        replace=True,
        aws_conn_id="aws_default"
    )

    create_raw_data_table = SnowflakeOperator(
        task_id="create_raw_data_table",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse="COMPUTE_WH",
        database="USGS_EARTHQUAKE_DB",
        schema="EARTHQUAKE_DATA",
        sql="snowflake/raw_data_table.sql",
        params={"table_name": SNOWFLAKE_RAW_DATA_TABLE}
    )

    load_to_snowflake = S3ToSnowflakeOperator(
        task_id="load_to_snowflake",
        snowflake_conn_id="snowflake_default",
        s3_keys=["earthquake-10-year-data.jsonl"], 
        table="raw_data",
        stage="my_s3_stage",
        file_format="(type = 'JSON', strip_outer_array = false)",
        warehouse="COMPUTE_WH",
        database="USGS_EARTHQUAKE_DB",
        schema="EARTHQUAKE_DATA",
    )

    create_heatmap_table = SnowflakeOperator(
        task_id="create_heatmap_table",
         snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse="COMPUTE_WH",
        database="USGS_EARTHQUAKE_DB",
        schema="EARTHQUAKE_DATA",
        sql="snowflake/heatmap_table.sql",
        params={"table_name": SNOWFLAKE_HEATMAP_TABLE}
    )

    create_country_stats_table = SnowflakeOperator(
        task_id="create_country_stats_table",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse="COMPUTE_WH",
        database="USGS_EARTHQUAKE_DB",
        schema="EARTHQUAKE_DATA",
        sql="snowflake/country_stats_table.sql",
        params={"table_name": SNOWFLAKE_COUNTRIES_TABLE}
    )

    fetch_task >> upload_task >> create_raw_data_table >> load_to_snowflake >> create_heatmap_table >> create_country_stats_table
