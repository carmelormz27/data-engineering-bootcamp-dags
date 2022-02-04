import airflow.utils.dates
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from gcs_to_postgres import GCSToPostgresTransfer

default_args = {
  'owner' : 'jose.ramirez',
  'depends_on_past' : False
  'start_date' : airflow.utils.dates.days_ago(1)
}

dag = DAG('dag_insert_data', default_args = default_args, schedule_interval = '@daily')

#Task to create SQL table
create_table = PostgresOperator(task_id = 'create_table',
                        sql="""
                        DROP TABLE dbname.purchases;
                        CREATE TABLE IF NOT EXISTS dbname.purchases
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
              schema = 'dbname',
              table = 'purchases',
              bucket = 'data-bootcamp-airflow',
              object = 'user_purchase.csv',
              google_cloud_conn_id = '',
              postgres_conn_id = '',
              dag = dag)

create_table >> upload_data