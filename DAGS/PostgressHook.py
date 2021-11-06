import airflow
import os
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta
from datetime import datetime
from psycopg2.extras import execute_values

#default arguments

default_args = {
    'owner': 'José Gallardo',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 1),
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




task1 = PostgresOperator(task_id = 'create_table',
                        sql="""
                        CREATE TABLE IF NOT EXISTS pokemon (
                            Number INTEGER,   
                            Name VARCHAR(255),
                            Type_1 VARCHAR(255),
                            Type_2 VARCHAR(255),
                            Total INTEGER,
                            HP INTEGER,
                            Attack INTEGER,
                            Defense INTEGER,
                            Sp_Atk INTEGER,
                            Sp_Def INTEGER,
                            Speed INTEGER,
                            Generation INTEGER,
                            Legendary VARCHAR(255));
                            """,
                            postgres_conn_id= 'conn_postgress',
                            autocommit=True,
                            dag = dag)

task1