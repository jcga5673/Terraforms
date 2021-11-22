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
}


#s3 = boto3.client('s3')

# define the python function
def my_function(x):
    return x + " is a must have tool for Data Engineers."

def read_csv(url,bucket):
    ##Download data and send it to s3
    path = 'https://drive.google.com/uc?export=download&id='+url.split('/')[-2]
    df = pd.read_csv(path)
    bucket_path_raw = bucket + 'raw.parquet'
    print(df.head(5))
    print(bucket_path_raw)
    #hook = S3Hook(aws_conn_id='conn_id').get_bucket('data-bootcamp-jose')
    df.to_parquet('raw.parquet')
    #hook.load_file( ,)
    client = boto3.client('s3') 
    #s3_resource = self.get_resource_type('s3')
    #s3 = boto3.resource('s3')
    ##Solution
    s3 = S3Hook(aws_conn_id='conn_id')
    s3.load_file(filename = 'raw.parquet',key = 'raw.parquet',bucket_name = 'data-bootcamp-jose')
    #s3.meta.client.upload_file('raw.parquet', 'data-bootcamp-jose', 'raw.parquet') ##acces denied
    print('check s3 please UwU')
    df = pd.read_parquet(bucket_path_raw)
    print(df.head(5))
    #df.to_parquet(bucket_path_raw)   #No module named s3fs
    #client.put_object('raw.parquet','data-bootcamp-jose','raw.parquet')
    #response = client.upload_file('raw.parquet', 'data-bootcamp-jose', 'raw.parquet')
    #hook.upload_file('raw.parquet','/raw.parquet','data-bootcamp-jose')
    return f"csv saved in parquet file in: {bucket_path_raw}"

def clear_data(bucket):
    ##read data from s3 and clean it
    bucket_path_raw = bucket + 'raw.parquet'
    bucket_path_stage = bucket + 'stage_data.csv'
    df = pd.read_parquet('raw.parquet')
    print(df.columns)

    for i,column in enumerate(df.columns):
        df[column] = df[column].astype(str)
        print(i,df[column])
        df[column] = df[column].str.replace(r'\W',"")
      
    df.to_parquet('stagin.parquet')

    try:
        s3 = boto3.resource('s3')
        s3.Bucket(bucket).download_file('raw.parquet', 'my_localimage.jpg')
        
    except Exception as err:
        print(err,'the error is here')

    return f"parquet saved into {bucket_path_stage}"
        

def send_data(data):
    try:
        conection = pg.connect(
            host = "terraform-2021112018360077700000000d.ctn9taanzupc.us-east-2.rds.amazonaws.com",
            user = "dbuser",
            password = "dbpassword",
            database = "dbname"
        )
        print('Conexión exitosa')
    except Exception as err:
        print(err,'no conection here brah')
    #df = pd.read_csv('data_frame.csv') 
    url = "https://drive.google.com/file/d/1ysfUdLi7J8gW6GDA3cOAbr7Zc4ZLhxxD/view?usp=sharing"
    path = 'https://drive.google.com/uc?export=download&id='+url.split('/')[-2]
    df = pd.read_csv(path)

    cur = conection.cursor()
    for i, row in df.iterrows():
        try:
            cur.execute("INSERT INTO user_purchase (invoice_number,stock_code, detail,quantity,invoice_date,unit_price,customer_id,country) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7]))
            print(i)
        except Exception as err:
            print(err,'solve this bro')

    return 'here is postgress: '



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

t2 = PythonOperator(
    task_id='clear_data',
    python_callable= clear_data,
    op_kwargs = {"bucket":"s3://data-bootcamp-jose/"},
    dag=dag,
)

t3 = PythonOperator(
    task_id = 'send_to_postgres',
    python_callable = send_data,
    op_kwargs={"data":'data_frame.parquet'},
    dag = dag,
)

'''
t4 = PostgresOperator(
    task_id ='Query_the_table',
    sql="SELECT * FROM user_purchase LIMIT 25",
    dag = dag,
)
'''

t1 >> t2 >> t3

