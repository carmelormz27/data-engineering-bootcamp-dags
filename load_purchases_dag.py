import airflow.utils.dates
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from gcs_to_postgres import GCSToPostgresTransfer

import datetime

default_args = {
  'owner' : 'jose.ramirez',
  'depends_on_past' : False,
  'start_date' : airflow.utils.dates.days_ago(1)
}

dag = DAG('dag_insert_data',default_args = default_args, schedule_interval = '@once', dagrun_timeout = datetime.timedelta(minutes=15))

#Task to create SQL table
create_table = PostgresOperator(task_id = 'create_table',
                        sql="""
                        CREATE TABLE IF NOT EXISTS databootcamp.user_purchase
                        (
                            id SERIAL PRIMARY KEY,
                            invoice_number VARCHAR(10),
                            stock_code VARCHAR(20),
                            detail VARCHAR(1000),
                            quantity INT,
                            invoice_date TIMESTAMP,
                            unit_price NUMERIC(8,3),
                            customer_id INT,
                            country VARCHAR(20)
                        );
                        """,
                        postgres_conn_id= 'postgres_default', 
                        autocommit=True,
                        dag= dag)

# Task to transfer data from GCS to Postgres DB
upload_data = GCSToPostgresTransfer(
              task_id = 'dag_gcs_to_postgres',
              schema = 'databootcamp',
              table = 'user_purchase',
              bucket = 'data-bootcamp-airflow',
              object = 'user_purchase.csv',
              google_cloud_conn_id = 'google_cloud_default',
              postgres_conn_id = 'postgres_default',
              dag = dag)

create_table >> upload_data