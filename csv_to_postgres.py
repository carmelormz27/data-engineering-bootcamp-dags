import airflow
import os
import psycopg2
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import airflow.utils.dates
from datetime import timedelta
from datetime import datetime

"""
  Load CSV to Postgres DB
"""

default_args = {
  'owner': 'Jose.Ramirez',
  'depends_on_past': False,
  'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('insert_csv_to_postgres', default_args = default_args, schedule_interval = '@once')

def file_path(relative_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    split_path = relative_path.split("/")
    new_path = os.path.join(dir, *split_path)
    return new_path

def csv_to_postgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    curr = get_postgres_conn.cursor()
    # CSV loading to table
    with open(file_path("user_purchase.csv"), "r") as f:
        next(f)
        curr.copy_from(f, "user_purchase", sep=",")
        get_postgres_conn.commit()

#Task to create SQL table
create_table = PostgresOperator(task_id = 'create_table',
                        sql="""
                        CREATE TABLE IF NOT EXISTS user_purchase
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

upload_data = PythonOperator(task_id = 'load_csv_to_database',
                              provide_context = True,
                              python_callable = csv_to_postgres,
                              dag=dag)


create_table >> upload_data
