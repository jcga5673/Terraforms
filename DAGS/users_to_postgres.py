import airflow
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from datetime import datetime
from airflow.hooks.S3_hook import S3Hook
from datetime import timedelta
import psycopg2 as pg
from airflow import DAG


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

def csv_to_postgres(bucket_file):
    hook = S3Hook(aws_conn_id="aws_default", verify=None)
    try:
        conection = pg.connect(
            host = "terraform-2022030816290704190000000f.cdzr8sg8du1x.us-east-2.rds.amazonaws.com",
            user = "dbuser",
            password = "dbpassword",
            database = "dbname"
        )
        print('Conexión exitosa')
    except Exception as err:
        print(err,'no conection here brah')
        return 0

    df = pd.read_csv(bucket_file)
    df['CustomerID'] = df['CustomerID'].fillna(10000)
    df['Description'] = df['Description'].fillna('No Description')
    df['CustomerID'] = df['CustomerID'].astype(int)
    for i, row in df.iterrows():
        try:
            cur.execute("INSERT INTO user_purchase (invoice_number,stock_code, detail,quantity,invoice_date,unit_price,customer_id,country) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7]))
            print(i)
        except Exception as err:
            print(err,'solve this bro')
            return 0
    print('good job')






dag = DAG('insert_data_postgres_dea',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

task1 = PythonOperator(task_id='csv_to_s3',
                        provide_context = True,
                        python_callable = csv_to_postgres,
                        op_kwargs={"s3":"s3://raw-data-dea-jose/user_purchase.csv"},
                        dag = dag)

task1