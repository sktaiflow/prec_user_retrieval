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
from airflow.models.variable import Variable
from airflow.providers.sktvane.operators.nes import NesOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors.web_hdfs_sensor import WebHdfsSensor
from airflow.utils import timezone
from airflow.utils.edgemodifier import Label


from macros.custom_slack import CallbackNotifier, SlackBot
from macros.custom_nes_task import create_nes_task

local_tz = pendulum.timezone("Asia/Seoul")

### AIRFLOW VARIABLE ###

env = Variable.get("env", "stg")
hdfs_root_path = Variable.get("hdfs_root_path", "/data/adot/jaehwan")
gcp_project_id = Variable.get("GCP_PROJECT_ID", "skt-datahub")
nudge_api_token = Variable.get("nudge_offering_token", None)
slack_conn_id = "slack_conn"
CallbackNotifier.SLACK_CONN_ID = slack_conn_id

## add Custom Variables
notebook_path = f"./domain_profile/adotServiceProfiles/notebook"

## add slack alarming task
ALARMING_TASK_IDS = [
    "start",
    "nudge_offering_table",
    "profile_adot",
    "profile_tdeal",
    "profile_tmap",
    "profile_xdr",
    "profile_xdr_weekend",
    "profile_adot_weekend",
    "profile_tmbr",
    "end_profiling",
]
CallbackNotifier.SELECTED_TASK_IDS = ALARMING_TASK_IDS

default_args = {
    "retries": 24,
    "depends_on_past": False,
    "retry_delay": timedelta(hours=1),
    "on_success_callback": CallbackNotifier.on_success_callback,
    "on_failure_callback": CallbackNotifier.on_failure_callback,
    "on_retry_callback": CallbackNotifier.on_retry_callback,
}

with DAG(
    dag_id=f"adotServiceProfiles_{env}",
    default_args=default_args,
    description="DAG with own plugins",
    schedule="0 22 * * *",
    start_date=pendulum.datetime(2024, 7, 11, tz=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=["adotServiceProfiles"],
) as dag:

    dag.doc_md = """User Profile && model Profile 만드는 DAG"""

    start = DummyOperator(task_id="start", dag=dag)
    end_profiling = DummyOperator(task_id="end_profiling", dag=dag)

    profile_adot = NesOperator(
        task_id="profile_adot",
        parameters={"current_dt": "{{ ds }}", "state": env, "log_duration": "60"},
        input_nb=f"{notebook_path}/profiling_adot.ipynb",
    )

    profile_adot_weekend = NesOperator(
        task_id="profile_adot_weekend",
        parameters={"current_dt": "{{ ds }}", "state": env, "log_duration": "60"},
        input_nb=f"{notebook_path}/profiling_adot_weekend.ipynb",
    )

    profile_tdeal = NesOperator(
        task_id="profile_tdeal",
        parameters={"current_dt": "{{ ds }}", "state": env, "log_duration": "60"},
        input_nb=f"{notebook_path}/profiling_tdeal.ipynb",
    )

    profile_tmap = NesOperator(
        task_id="profile_tmap",
        parameters={"current_dt": "{{ ds }}", "state": env, "log_duration": "60"},
        input_nb=f"{notebook_path}/profiling_tmap.ipynb",
    )
    profile_xdr = NesOperator(
        task_id="profile_xdr",
        parameters={"current_dt": "{{ ds }}", "state": env, "log_duration": "30"},
        input_nb=f"{notebook_path}/profiling_xdr.ipynb",
    )

    profile_xdr_weekend = NesOperator(
        task_id="profile_xdr_weekend",
        parameters={"current_dt": "{{ ds }}", "state": env, "log_duration": "60"},
        input_nb=f"{notebook_path}/profiling_xdr_weekend.ipynb",
    )

    profile_tmbr = NesOperator(
        task_id="profile_tmbr",
        parameters={"current_dt": "{{ ds }}", "state": env, "log_duration": "60"},
        input_nb=f"{notebook_path}/profiling_tmbr.ipynb",
    )

    nudge_offering_table = NesOperator(
        task_id="nudge_offering_table",
        parameters={
            "current_dt": "{{ ds }}",
            "state": env,
            "log_duration": "60",
            "nudge_token": nudge_api_token,
        },
        input_nb=f"{notebook_path}/nudge_offering_table.ipynb",
    )

    # profilePivotTable =  NesOperator(
    #     task_id="profilePivotTable",
    #     parameters={"current_dt": "{{ ds }}", "state": env},
    #     input_nb="./domain_profile/adotServiceProfiles/notebook/pivoting_profile.ipynb",
    # )

    start >> nudge_offering_table >> end_profiling
    (
        start
        >> [
            profile_adot,
            profile_tdeal,
            profile_tmap,
            profile_xdr,
            profile_tmbr,
            profile_xdr_weekend,
            profile_adot_weekend,
        ]
        >> end_profiling
    )
