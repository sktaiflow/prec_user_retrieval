from __future__ import annotations

from datetime import datetime, timedelta, time
from textwrap import dedent

import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTablePartitionExistenceSensor,
)
from airflow.models.variable import Variable

from airflow.providers.sktvane.operators.nes import NesOperator
from airflow.utils import timezone
from airflow.utils.edgemodifier import Label
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.providers.sktvane.macros.gcp import bigquery_client

from macros.custom_slack import CallbackNotifier, SlackBot
from macros.custom_nes_task import create_nes_task


### AIRFLOW VARIABLE ###
local_tz = pendulum.timezone("Asia/Seoul")
conn_id = "slack_conn"
env = Variable.get("env", "stg")
project_id = Variable.get("GCP_PROJECT_ID", "skt-datahub")
db_name = "adot_reco"
slack_conn_id = "slack_conn"
CallbackNotifier.SLACK_CONN_ID = conn_id

## add Custom Variables
common_process_notebook_path = "./common/preprocessing/notebook"
log_process_path = f"{common_process_notebook_path}/log"
## add slack alarming task
ALARMING_TASK_IDS = [
    "start",
    "xdr_cat1_cnt",
    "tmap_item_cnt",
    "tmap_cat1_cnt",
    "adot_cat1_cnt",
    "adot_item_cnt",
    "tdeal_cat1_cnt",
    "st11_cat1_cnt",
    "tmbr_cat1_cnt",
    "end",
]
CallbackNotifier.SELECTED_TASK_IDS = ALARMING_TASK_IDS

default_args = {
    "retries": 24,
    "depends_on_past": True,
    "retry_delay": timedelta(hours=1),
    "on_success_callback": CallbackNotifier.on_success_callback,
    "on_failure_callback": CallbackNotifier.on_failure_callback,
    "on_retry_callback": CallbackNotifier.on_retry_callback,
}

common_process_notebook_path = "./common/preprocessing/notebook"
log_process_path = f"{common_process_notebook_path}/log"
meta_process_path = f"{common_process_notebook_path}/meta"


with DAG(
    dag_id=f"CommonPreprocessProfile_{env}",
    default_args=default_args,
    description="DAG with own plugins",
    schedule="0 22 * * *",
    start_date=pendulum.datetime(2024, 7, 15, tz=local_tz),
    catchup=True,
    max_active_runs=1,
    tags=["CommonPreprocessProfiles"],
) as dag:

    dag.doc_md = """PROFILE 만들기 위한 도메인별 log 공통 전처리 DAG"""

    end = DummyOperator(task_id="end")
    start = DummyOperator(task_id="start")

    xdr_cat1_cnt = NesOperator(
        task_id="xdr_cat1_cnt",
        parameters={"current_dt": "{{ ds }}", "state": env, "ttl": "60"},
        input_nb=f"{log_process_path}/p_xdr_cat1.ipynb",
    )

    tmap_item_cnt = NesOperator(
        task_id="tmap_item_cnt",
        parameters={"current_dt": "{{ ds }}", "state": env, "ttl": "60"},
        input_nb=f"{log_process_path}/p_tmap_item.ipynb",
    )
    tmap_cat1_cnt = NesOperator(
        task_id="tmap_cat1_cnt",
        parameters={"current_dt": "{{ ds }}", "state": env, "ttl": "60"},
        input_nb=f"{log_process_path}/p_tmap_cat1.ipynb",
    )

    st11_cat1_cnt = NesOperator(
        task_id="st11_cat1_cnt",
        parameters={"current_dt": "{{ ds }}", "state": env, "ttl": "60"},
        input_nb=f"{log_process_path}/p_11st_cat1.ipynb",
    )

    tdeal_cat1_cnt = NesOperator(
        task_id="tdeal_cat1_cnt",
        parameters={"current_dt": "{{ ds }}", "state": env, "ttl": "60"},
        input_nb=f"{log_process_path}/p_tdeal_cat1.ipynb",
    )

    adot_cat1_cnt = NesOperator(
        task_id="adot_cat1_cnt",
        parameters={"current_dt": "{{ macros.ds_add(ds, 2) }}", "state": env, "ttl": "30"},
        input_nb=f"{log_process_path}/p_adot_cat1.ipynb",
    )

    adot_item_cnt = NesOperator(
        task_id="adot_item_cnt",
        parameters={"current_dt": "{{ macros.ds_add(ds, 2) }}", "state": env, "ttl": "30"},
        input_nb=f"{log_process_path}/p_adot_item.ipynb",
    )

    tmbr_cat1_cnt = NesOperator(
        task_id="tmbr_cat1_cnt",
        parameters={"current_dt": "{{ ds }}", "state": env, "ttl": "60"},
        input_nb=f"{log_process_path}/p_tmbr_cat1.ipynb",
    )

    """DAG CHAIN"""

    (
        start
        >> [
            xdr_cat1_cnt,
            tmap_item_cnt,
            tmap_cat1_cnt,
            adot_cat1_cnt,
            adot_item_cnt,
            tdeal_cat1_cnt,
            st11_cat1_cnt,
            tmbr_cat1_cnt,
        ]
        >> end
    )
