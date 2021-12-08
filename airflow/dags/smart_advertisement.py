from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable


from operators import LoadToRedshiftOperator, DataQualityOperator


default_args = {
    "owner": "add_smart",
    "depends_on_past": False,
    "start_date": datetime.now() - timedelta(days=1),
    "retries": 1,
    "retries_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG(
    "smart_advertisement",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@monthly",
    max_active_runs=1,
)

# Create dummy operator
start_operator = DummyOperator(task_id="begin_execution", dag=dag)

# create all tables using create_tables.sql
create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql="create_tables.sql",
    postgres_conn_id="redshift",
)

# Load immigration data to redshift
load_immigration_task = LoadToRedshiftOperator(
    task_id="load_immigration_data",
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    table="immigration",
    s3_key="immigration",
    format_as="PARQUET",
    dag=dag,
)

# Load demographic data to redshift
load_demographic_task = LoadToRedshiftOperator(
    task_id="load_demographic_data",
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    table="demographics",
    s3_key="demographic",
    format_as="PARQUET",
    dag=dag,
)

# load airports data to redshift
load_airports_task = LoadToRedshiftOperator(
    task_id="load_airports_data",
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    table="airports",
    s3_key="airport",
    format_as="PARQUET",
    dag=dag,
)

run_quality_checks = DataQualityOperator(
    task_id="run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    tables=["airports", "demographics"],
)

end_operator = DummyOperator(task_id="stop_execution", dag=dag)

# Ordering the task in the pipeline
start_operator >> create_tables_task

create_tables_task >> load_immigration_task
create_tables_task >> load_demographic_task
create_tables_task >> load_airports_task

load_immigration_task >> run_quality_checks
load_demographic_task >> run_quality_checks
load_airports_task >> run_quality_checks

run_quality_checks >> end_operator
