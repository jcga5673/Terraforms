### importing the required libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
#from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
import psycopg2 as pg
import sqlalchemy
import boto3
import fsspec
import tempfile


default_args = {
    'owner': 'José',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['pepepito@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


#s3 = boto3.client('s3')

# define the python function
def my_function(x):
    return x + " is a must have tool for Data Engineers."

def read_csv(url,bucket):
    ##Download data and send it to s3
    path = 'https://drive.google.com/uc?export=download&id='+url.split('/')[-2]
    df = pd.read_csv(path)
    bucket_path_raw = bucket + 'raw_data.csv'
    print(df.head(5))
    print(bucket_path_raw)
    #hook = S3Hook(aws_conn_id='conn_id').get_bucket('data-bootcamp-jose')
    #hook.load_file( ,)

    #s3_resource = self.get_resource_type('s3')

    #s3 = boto3.client('s3',aws_access_key_id = '',aws_secret_access_key='')
    #bucket = 'data_bootcamp'
    #bucket = 's3://'
    #df.to_parquet(bucket_path_raw)
    df.to_parquet('raw.parquet')
    #hook.load_file('raw.parquet','raw.parquet','data-bootcamp.jose')
    return f"csv saved in parquet file in: {bucket_path_raw}"



# define the DAG
dag = DAG(
    'python_operator_sample',
    default_args=default_args,
    description='How to use the Python Operator',
    schedule_interval=timedelta(days=1),
)

# define the first task




t1 = PythonOperator(
    task_id = 'read_csv',
    python_callable = read_csv,
    op_kwargs={"url":"https://drive.google.com/file/d/1ysfUdLi7J8gW6GDA3cOAbr7Zc4ZLhxxD/view?usp=sharing","bucket":"s3://data-bootcamp-jose/"},
    dag = dag,
)



t1 