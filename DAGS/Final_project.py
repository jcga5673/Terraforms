from datetime import datetime, timedelta,date
#import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
#from airflow.contrib.operators.s3_list_operator import s3_list_operator
#from custom_modules.dag_s3_to_postgres import S3ToPostgresTransfer
from airflow.hooks.S3_hook import S3Hook
import boto3
import os.path
import pandas as pd
import io



# Configurations
BUCKET_NAME = "data-bootcamp-jose"  # replace this with your bucket name
#local_data = "./dags/data/movie_review.csv"
s3_data_movie = "Data/movie_review.csv"
s3_data_user= "Data/user_purchase.csv"
#local_script = "./dags/scripts/spark/random_text_classification.py"
s3_script = "final_pyspark_copy.py"#final_pyspark_code.py"

s3_clean = "clean_data/"


timestamp = date.today()#datetime.now()


#"spark-submit",
#"--deploy-mode",
#"client",
#"s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",


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
                "--date={{params.time_stamp}}"
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


def list_s3():

        hook = S3Hook(aws_conn_id="aws_default", verify=None)

        return hook.list_keys(
            bucket_name='data-bootcamp-jose',
            prefix="final_result/")


dag = DAG(
    "final_etl_project",
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
        "s3_data_movie": s3_data_movie,
        "s3_data_user": s3_data_user,
        "s3_script": s3_script,
        "time_stamp": timestamp
        #"s3_clean": s3_clean,
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

list_bucket = PythonOperator(
    task_id = 'list_objects',
    python_callable = list_s3,
    #op_kwargs={"bucket":"s3://data-bootcamp-jose/"},
    dag = dag

    )



transfer_s3_to_redshift = S3ToRedshiftOperator(
    s3_bucket=BUCKET_NAME,
    s3_key="Data",
    schema="PUBLIC",
    table="user_behavior_metric",
    copy_options=['csv'],
    task_id='transfer_s3_to_redshift',
    dag=dag,
)


end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)


start_data_pipeline >>  create_emr_cluster
create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
terminate_emr_cluster >> list_bucket  >> transfer_s3_to_redshift
transfer_s3_to_redshift >> end_data_pipeline