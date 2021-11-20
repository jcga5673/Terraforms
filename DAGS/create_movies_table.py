from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

dag_params = {
    'dag_id': 'PostgresOperator_dag',
    'start_date': datetime(2021, 1, 29),
    'schedule_interval': None
}

with DAG(**dag_params) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        sql='''CREATE TABLE IF NOT EXISTS movies_reviews(
            cid int,
            positive_review int)
            ''',
        postgres_conn_id= 'conn_postgress',
        autocommit=True,
    )
    