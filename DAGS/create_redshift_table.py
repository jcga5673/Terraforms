from os import getenv

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.dates import days_ago




dag_params = {
    'dag_id': 'redshift_table',
    'start_date': datetime(2021, 1, 29),
    'schedule_interval': None
}

with DAG(**dag_params) as dag:

    setup__task_create_table = RedshiftSQLOperator(
        sql=f'CREATE TABLE IF NOT EXISTS user_behavior_metric(costumerid int, amount_spent int,review_score int, review_count int)',
        task_id='create_table',
    )