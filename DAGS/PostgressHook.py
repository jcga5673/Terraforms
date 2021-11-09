import airflow
import os
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta
from datetime import datetime
from psycopg2.extras import execute_values
import pandas as pd

#default arguments

default_args = {
    'owner': 'José Gallardo',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email': ['jcga5673@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG('insert_data_postgres',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

def csv_to_postgres(url):
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='conn_postgress')
    get_postgres_conn = PostgresHook(postgres_conn_id='conn_postgress').get_conn()
    curr = get_postgres_conn.cursor("cursor")
    # CSV loading to table.

    # Getting the current work directory (cwd)
    path = 'https://drive.google.com/uc?export=download&id='+url.split('/')[-2]
    df = pd.read_csv(path)
    print(df.head(5))
    df.to_parquet('data_frame.csv')
    for i, row in df.iterrows():
        try:
            curr.execute("INSERT INTO user_purchase (invoice_number,stock_code, detail,quantity,invoice_date,unit_price,customer_id,country) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7]))
        except Exception as err:
            print(err,'solve this bro')
        if i == 50:
            break
    return 'here is postgress: ' + data



task1 = PythonOperator(task_id='csv_to_s3',
                        provide_context = True,
                        python_callable = csv_to_postgres,
                        op_kwargs={"url":"https://drive.google.com/file/d/1ysfUdLi7J8gW6GDA3cOAbr7Zc4ZLhxxD/view?usp=sharing"},
                        dag = dag)

task1