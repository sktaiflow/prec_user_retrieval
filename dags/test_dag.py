from __future__ import annotations

from datetime import datetime, timedelta
from textwrap import dedent

import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.models.variable import Variable

from macros.custom_slack import CallbackNotifier
from macros.custom_nes_task import create_nes_task
from macros.airflow_variables_templates import create_airflow_variables_enum, DefaultVariables


local_tz = pendulum.timezone("Asia/Seoul")


default_args = {
    "retries": 24,
    "depends_on_past": False,
    "retry_delay": timedelta(hours=1),
}

with DAG(
    dag_id=f"test--stg",
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
