from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


dag_params = {
    'dag_id': 'create_movies_table',
    'start_date': datetime(2021, 1, 29),
    'schedule_interval': None
}

with DAG(**dag_params) as dag:

    this_will_skip = BashOperator(
    task_id='this_will_skip',
    bash_command='aws redshift-data execute-statement --database my_database --db-user adminuser --cluster-identifier mycluster --region us-east-2 --sql "CREATE TABLE movie_review (cid int,positive_review int);"',
    dag=dag,
    )       
    