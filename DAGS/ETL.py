### importing the required libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import pandas as pd
import psycopg2 as pg
import sqlalchemy


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


# define the python function
def my_function(x):
    return x + " is a must have tool for Data Engineers."

def read_csv(url):
    path = 'https://drive.google.com/uc?export=download&id='+url.split('/')[-2]
    df = pd.read_csv(path)
    print(df.head(5))
    df.to_parquet('data_frame.csv')
    return "csv saved into data-frame.parquet"

def send_data(data):
    try:
        conection = pg.connect(
            host = "postgres",
            user = "airflow",
            password = "airflow",
            database = "airflow"
        )
        print('Conexión exitosa')
    except Exception as err:
        print(err,'no conection here brah')
    #df = pd.read_csv('data_frame.csv') 
    url = "https://drive.google.com/file/d/1ysfUdLi7J8gW6GDA3cOAbr7Zc4ZLhxxD/view?usp=sharing"
    path = 'https://drive.google.com/uc?export=download&id='+url.split('/')[-2]
    df = pd.read_csv(path)
    #engine = sqlalchemy.create_engine('postgresql://airflow:airflow@localhost:/postgres')
    cur = conection.cursor()
    for i, row in df.iterrows():
        try:
            cur.execute("INSERT INTO user_purchase (invoice_number,stock_code, detail,quantity,invoice_date,unit_price,customer_id,country) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7]))
        except Exception as err:
            print(err,'solve this bro')
        if i == 50:
            break
    return 'here is postgress: ' + data



# define the DAG
dag = DAG(
    'python_operator_sample',
    default_args=default_args,
    description='How to use the Python Operator',
    schedule_interval=timedelta(days=1),
)

# define the first task
'''
t2 = PythonOperator(
    task_id='print',
    python_callable= my_function,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag,
)
'''

t1 = PythonOperator(
    task_id = 'read_csv',
    python_callable = read_csv,
    op_kwargs={"url":"https://drive.google.com/file/d/1ysfUdLi7J8gW6GDA3cOAbr7Zc4ZLhxxD/view?usp=sharing"},
    dag = dag,
)

t2 = PythonOperator(
    task_id = 'send_to_postgres',
    python_callable = send_data,
    op_kwargs={"data":'data_frame.parquet'},
    dag = dag,
)


t3 = PostgresOperator(
    task_id ='Query_the_table',
    sql="SELECT * FROM user_purchase LIMIT 25",
    dag = dag,
)


t1 >> t2 >> t3

#t1 >> t2