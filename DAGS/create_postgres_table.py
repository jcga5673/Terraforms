from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

dag_params = {
    'dag_id': 'movie_table_postgres_dea',
    'start_date': datetime(2021, 1, 29),
    'schedule_interval': None
}

with DAG(**dag_params) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        sql='''CREATE TABLE IF NOT EXISTS user_purchase(
                InvoiceNo  varchar(100),
                StockCode varchar(100),
                Description varchar(100),
                Quantity int,
                InvoiceDate varchar(50),
                UnitPrice float,
                CustomerID int,
                Country varchar(50))
            ''',
        postgres_conn_id= 'conn_postgress',
        autocommit=True,
    )
    