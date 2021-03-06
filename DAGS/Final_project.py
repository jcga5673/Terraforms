from datetime import datetime, timedelta, date
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



# Configurations
BUCKET_NAME = "data-bootcamp-jose"  
s3_data_movie = "Data/movie_review.csv"
s3_data_user= "Data/user_purchase.csv"
bucket_key = "final_result"
s3_script = "final_pyspark_copy.py"
s3_clean = "clean_data/"


# timestamp to register insertion date
time_stamp = str(date.today())


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
    "owner": "Jos?? Gallardo",
    "start_date": datetime(2021, 1, 1),
    "email": ["jose567345@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def list_s3(bucket_name,prefix) -> str:
    hook = S3Hook(aws_conn_id="aws_default", verify=None)
    list_keys = hook.list_keys(bucket_name=bucket_name,prefix='final_result')
    return list_keys[1]


dag = DAG(
    "final_etl_project",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)


create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    region_name = "us-east-2",
    dag=dag,
)


run_pyspark_code = EmrAddStepsOperator(
    task_id="run_pyspark_code",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "s3_data_movie": s3_data_movie,
        "s3_data_user": s3_data_user,
        "s3_script": s3_script,
        "time_stamp": time_stamp
    },
    dag=dag,
)


emr_sensor = EmrStepSensor(
    task_id="emr_sensor",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(0)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)


terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)


get_s3_objects_names = PythonOperator(
    task_id = 'get_s3_objects_names',
    python_callable = list_s3,
    op_kwargs = {'bucket_name': BUCKET_NAME,'prefix': "final_result/"},
    dag = dag
    )


transfer_s3_to_redshift = S3ToRedshiftOperator(
    s3_bucket=BUCKET_NAME,
    s3_key="{{ task_instance.xcom_pull(task_ids='list_objects') }}",
    schema="public",
    table="user_behavior_metric",
    copy_options=['csv'],
    task_id='transfer_s3_to_redshift',
    dag=dag,
)


end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)


start_data_pipeline >>  create_emr_cluster
create_emr_cluster >> run_pyspark_code >> emr_sensor >> terminate_emr_cluster
terminate_emr_cluster >> get_s3_objects_names  >> transfer_s3_to_redshift
transfer_s3_to_redshift >> end_data_pipeline