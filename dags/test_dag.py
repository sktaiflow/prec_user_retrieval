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

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

from macros.custom_slack import CallbackNotifier
from macros.custom_nes_task import create_nes_task
from macros.airflow_variables_templates import create_airflow_variables_enum, DefaultVariables


local_tz = pendulum.timezone("Asia/Seoul")

## GET AIRFLOW VARIABLE ###
extra_variables = {}
airflow_vars = create_airflow_variables_enum(
    DefaultVariables().update_variables_from_dict(extra_variables)
)

print(airflow_vars)

env = Variable.get("env", "stg")
hdfs_root_path = Variable.get("hdfs_root_path", "/data/adot/jaehwan")
gcp_project_id = Variable.get("GCP_PROJECT_ID", "skt-datahub")
nudge_api_token = Variable.get("nudge_offering_token", None)
slack_conn_id = "slack_conn"
CallbackNotifier.SLACK_CONN_ID = slack_conn_id

## add Custom Variables
notebook_path = f"./domain_profile/adotServiceProfiles/notebook"
db_name = "adot_reco_dev"

## add slack alarming task
ALARMING_TASK_IDS = []

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
    dag_id=f"test--{env}",
    default_args=default_args,
    description="DAG with own plugins",
    schedule="0 15 * * *",
    start_date=pendulum.datetime(2024, 7, 11, tz=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=["test"],
) as dag:

    dag.doc_md = """User Profile && model Profile 만드는 DAG"""

    start = DummyOperator(task_id="start", dag=dag)
    end = DummyOperator(task_id="end", dag=dag)
    start >> end
