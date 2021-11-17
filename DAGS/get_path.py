### importing the required libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import pandas as pd

'''

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
def path_local():
    # Get the current working directory
    cwd = os.getcwd()

    # Print the current working directory
    print("Current working directory: {0}".format(cwd))

    files = os.listdir('/opt/airflow/dags')
    print('hi')
    for f in files:
        print(f)

    #df = pd.read_csv('/opt/airflow/dags/movie_review.csv')
    #print(df.head)
    return  "work there pliz"





# define the DAG
dag = DAG(
    'get_path',
    default_args=default_args,
    description='Hardcoder the path xD',
    schedule_interval=timedelta(days=1),
)

# define the first task




t1 = PythonOperator(
    task_id = 'path_python',
    python_callable = path_local,
    dag = dag,
)
'''

from airflow import DAG
from airflow.settings import AIRFLOW_HOME
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

dag = DAG(
    dag_id='where_is_home',
    start_date=datetime.now()
)

def command():

    print("$AIRFLOW_HOME=", AIRFLOW_HOME)

with dag:
    po = PythonOperator(
        task_id='print',
        python_callable=command,
        provide_context=True
    )

