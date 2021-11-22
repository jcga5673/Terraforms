from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import os.path
import pandas as pd
import io



# Configurations
BUCKET_NAME = "data-bootcamp-jose"  # replace this with your bucket name
local_data = "./dags/data/movie_review.csv"
s3_data = "movie_review.csv"
local_script = "./dags/scripts/spark/random_text_classification.py"
s3_script = "positive_reviews.py"
s3_clean = "clean_data/"

SPARK_STEPS = [    
    {
        "Name": "Classify movie reviews",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
            ],
        },
    }
]    


JOB_FLOW_OVERRIDES = {
    "Name": "Movie review classifier",
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


def send_to_rds():
    pg_hook = PostgresHook(postgre_conn_id = 'postgres_default')
    s3 = S3Hook(aws_conn_id = 'aws_default', verify = None)
    s3_key = '/result.csv/part-00000-6c40b5bd-9f60-460e-aacb-c4a39f84e6c3-c000.csv'
    s3_bucket = 'data-bootcamp-jose'
    print('Dowbnloading s3 file')
    

    if not self.s3.check_for_wildcard_key(self.s3_key, self.s3_bucket):
        raise AirflowException("No key matches {0}".format(self.s3_key))
    s3_key_object = self.s3.get_wildcard_key(self.s3_key, self.s3_bucket)

    s3_key_object = s3.get_wildcard_key(s3_key, s3_bucket)
    list_srt_content = s3_key_object.get()['Body'].read().decode(encoding = "utf-8", errors = "ignore")
    
    df = pd.read_csv(io.StringIO(list_srt_content))
    print(df.head(5))
    schema = {
        'cid':  'int',
        'positive_review': 'int'
    }

    return "let's keep trying"

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2020, 10, 17),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spark_submit_airflow",
    default_args=default_args,
    schedule_interval="0 10 * * *",
    max_active_runs=1,
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    region_name = "us-east-2",
    dag=dag,
)

# Add your steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "s3_data": s3_data,
        "s3_script": s3_script,
        "s3_clean": s3_clean,
    },
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

task_transfer_s3_to_rds = PythonOperator(
    task_id = 'send_to_rds',
    python_callable = send_to_rds,
    #op_kwargs={},
    dag = dag
)



end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)


start_data_pipeline >>  create_emr_cluster
create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
terminate_emr_cluster >> task_transfer_s3_to_rds >> end_data_pipeline