from __future__ import annotations

from datetime import datetime, timedelta
from textwrap import dedent

import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.models.variable import Variable
from airflow.operators.python_operator import PythonOperator

from macros.custom_slack import CallbackNotifier
from macros.custom_nes_task import create_nes_task
from macros.airflow_variables_templates import create_airflow_variables_enum, DefaultVariables
import logging


extra_variables = {}
airflow_vars = create_airflow_variables_enum(
    DefaultVariables().update_variables_from_dict(extra_variables)
)

logger = logging.getLogger(__name__)
logger.info(f"This is a log message: {airflow_vars}")

print(airflow_vars)

env = Variable.get("env", "stg")
hdfs_root_path = Variable.get("hdfs_root_path", "/data/adot/jaehwan")
gcp_project_id = Variable.get("GCP_PROJECT_ID", "skt-datahub")
nudge_api_token = Variable.get("nudge_offering_token", None)
slack_conn_id = "slack_conn"
CallbackNotifier.SLACK_CONN_ID = slack_conn_id

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

    dag.doc_md = """tests for custom plugins"""

    def print_airflow_vars(airflow_vars):
        # Assuming 'airflow_vars' is a dictionary containing Airflow variables
        for key, value in airflow_vars.items():
            print(f"{key}: {value}")

    print_vars_operator = PythonOperator(
        task_id="print_airflow_vars",
        python_callable=print_airflow_vars,
        op_kwargs={"airflow_vars": airflow_vars},
        dag=dag,  # Ensure 'dag' is defined in your DAG file
    )

    start = DummyOperator(task_id="start", dag=dag)
    end = DummyOperator(task_id="end", dag=dag)
    start >> print_vars_operator >> end
