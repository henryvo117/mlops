from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

local_tz = pendulum.timezone("Asia/Tehran")

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['hottestcun@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(dag_id='analysis_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="30 * * * *")
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
click_stream_delta_lag_alert = SparkSubmitOperator(task_id='analysis_sensor',
                                                   conn_id='spark_default',
                                                   application=f'{pyspark_app_home}/analysis.py',
                                                   total_executor_cores=4,
                                                   executor_cores=2,
                                                   executor_memory='5g',
                                                   driver_memory='5g',
                                                   name='analysis_sensor',
                                                   dag=dag,
                                                   )
