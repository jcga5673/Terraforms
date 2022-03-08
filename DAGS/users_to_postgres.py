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

def csv_to_postgres(url):

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

    df = pd.read_csv('https://drive.google.com/uc?export=download&id='+url.split('/')[-2])
    df['CustomerID'] = df['CustomerID'].fillna(10000)
    df['Description'] = df['Description'].fillna('No Description')
    df['CustomerID'] = df['CustomerID'].astype(int)
    cur = conection.cursor()
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
                        op_kwargs={"url":"https://drive.google.com/file/d/1ysfUdLi7J8gW6GDA3cOAbr7Zc4ZLhxxD/view?usp=sharing"},
                        dag = dag)

task1