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
        sql='CREATE TABLE IF NOT EXISTS user_behavior_metric1(costumerid bigint, amount_spent bigint,review_score bigint, review_count bigint,insert_date varchar(12))',
        postgres_conn_id= 'redshift_default',
        autocommit=True,
    )

