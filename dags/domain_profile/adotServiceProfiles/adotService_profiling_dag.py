"""
### DAG Documentation
이 DAG는 HivePartitionSensor를 사용하는 예제입니다.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from textwrap import dedent

import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTablePartitionExistenceSensor,
)
from airflow.providers.sktvane.operators.nes import NesOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors.web_hdfs_sensor import WebHdfsSensor
from airflow.utils import timezone
from airflow.utils.edgemodifier import Label

##
from macros.jh_slack import CallbackNotifier
from operators.custom_operators import BigQueryDoublePartitionExistenceSensor

local_tz = pendulum.timezone("Asia/Seoul")

### 
conn_id = 'slack_conn'
CallbackNotifier.SLACK_CONN_ID = conn_id

env='stg'
aidp_project_id = "skt-datahub"

SELECTED_TASK_IDS = []
SELECTED_TASK_IDS.extend(["preprocess_adot", "preprocess_edd", "preprocess_edd_lag", "recgpt_item_list", "recgpt_item_list_train", "build_vocab", "onemodelV3_input_train"])
CallbackNotifier.SELECTED_TASK_IDS = SELECTED_TASK_IDS

###


default_args = {
    "retries": 100,
    "on_success_callback" :  CallbackNotifier.on_success_callback,
    "on_failure_callback": CallbackNotifier.on_failure_callback,
    "on_retry_callback": CallbackNotifier.on_retry_callback,
    "depends_on_past": True
}


with DAG(
    dag_id=f"adotServiceProfiles_{env}",
    default_args=default_args,
    description="DAG with own plugins",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2024, 6, 22, tz=local_tz),
    catchup=True,
    max_active_runs=1,
    tags=["onemodel_v3_datapipe_test"],
    
) as dag: 
    
    start = DummyOperator(task_id='start', dag=dag)
    end_profiling = DummyOperator(task_id='end_preprocess', dag=dag)
    #end = DummyOperator(task_id='end', dag=dag)

    profile_adot =  NesOperator(
        task_id="profile_adot",
        parameters={"current_dt": "{{ ds }}", "state": "stg", "duration": "30"},
        input_nb="./notebook/profiling_adot.ipynb",
    )
    profile_tdeal =  NesOperator(
        task_id="profile_tdeal",
        parameters={"current_dt": "{{ ds }}", "state": "stg", "duration": "30"},
        input_nb="./notebook/profiling_tdeal.ipynb",
    )

    profile_tmap =  NesOperator(
        task_id="profile_tmap",
        parameters={"current_dt": "{{ ds }}", "state": "stg", "duration": "30"},
        input_nb="./notebook/profiling_tmap.ipynb",
    )
    profile_xdr =  NesOperator(
        task_id="profile_xdr",
        parameters={"current_dt": "{{ ds }}", "state": "stg", "duration": "30"},
        input_nb="./notebook/profiling_xdr.ipynb",
    )
    start >> [profile_adot, profile_tdeal, profile_tmap, profile_xdr] >> end_profiling

