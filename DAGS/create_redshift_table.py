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
        sql='CREATE TABLE IF NOT EXISTS user_behavior_metric(costumerid int, amount_spent int,review_score int, review_count int,insert_date DATE)',
        postgres_conn_id= 'conn_redshift',
        autocommit=True,
    )
