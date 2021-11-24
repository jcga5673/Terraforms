from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator




dag_params = {
    'dag_id': 'redshift_table',
    'start_date': datetime(2021, 1, 29),
    'schedule_interval': None
}

with DAG(**dag_params) as dag:

    setup__task_create_table = RedshiftSQLOperator(
        sql='CREATE TABLE IF NOT EXISTS user_behavior_metric(costumerid int, amount_spent int,review_score int, review_count int)',
        task_id='create_table',
    )