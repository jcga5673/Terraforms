from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator




dag_params = {
    'dag_id': 'redshift_table',
    'start_date': datetime(2021, 1, 29),
    'schedule_interval': None
}

with DAG(**dag_params) as dag:

    create_table = PostgresOperator(
        task_id='create_redshift_table',
        sql='CREATE TABLE IF NOT EXISTS user_behavior_metric1(costumerid varchar(50), amount_spent varchar(50),review_score varchar(50), review_count varchar(50),insert_date varchar(50))',
        postgres_conn_id= 'conn_redshift',
        autocommit=True,
    )
