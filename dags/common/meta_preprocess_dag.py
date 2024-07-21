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
env = Variable.get("env", "stg")
project_id = Variable.get("GCP_PROJECT_ID", "skt-datahub")
db_name = "adot_reco"
slack_conn_id = "slack_conn"
CallbackNotifier.SLACK_CONN_ID = slack_conn_id
## add Custom Variables

common_process_notebook_path = "./common/preprocessing/notebook"
meta_process_path = f"{common_process_notebook_path}/meta"
tmbr_meta_original_table = "mp_taxonomies_brand"

## add slack alarming task
ALARMING_TASK_IDS = [
    "start",
    "create_tmbr_etymology_table",
    "create_basic_category_table",
    "create_final_category_table",
    "create_tmbr_meta_active_table",
    "tmbr_meta_table",
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

with DAG(
    dag_id=f"CommonMetaPreprocess_{env}",
    default_args=default_args,
    description="DAG with own plugins",
    schedule="0 22 * * *",
    start_date=pendulum.datetime(2024, 7, 11, tz=local_tz),
    catchup=True,
    max_active_runs=1,
    tags=["CommonMetaPreprocess"],
) as dag:

    dag.doc_md = """META 만드는 DAG"""

    def check_meta_update(**kwargs):
        hook = BigQueryHook(bigquery_conn_id="bigquery_default", use_legacy_sql=False)
        sql = f"""
            SELECT COUNT(*) as count
            FROM {project_id}.{db_name}.{tmbr_meta_original_table}
            WHERE updated_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) OR created_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
            AND your_condition
        """
        result = hook.get_first(sql)
        if result and result[0] > 0:
            return "tmbr_meta_table"
        else:
            return "end"

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    create_tmbr_etymology_table = BigQueryCreateEmptyTableOperator(
        task_id="create_tmbr_etymology_table",
        dataset_id=db_name,
        table_id="tmbr_etymology_table",
        schema_fields=[
            {"name": "brand_name", "type": "STRING"},
            {"name": "description", "type": "STRING"},
            {"name": "derivative_brand", "type": "STRING"},
            {"name": "dt", "type": "DATE"},
        ],
        time_partitioning={
            "field": "dt",
            "type": "DAY",
        },
        exists_ok=True,
    )
    create_basic_category_table = BigQueryCreateEmptyTableOperator(
        task_id="create_basic_category_table",
        dataset_id=db_name,
        table_id="tmbr_basic_category_mapping_table",
        schema_fields=[
            {"name": "brand_name", "type": "STRING"},
            {"name": "category_large_name", "type": "STRING"},
            {"name": "category_medium_name", "type": "STRING"},
            {"name": "category_small_name", "type": "STRING"},
            {"name": "description", "type": "STRING"},
            {"name": "dt", "type": "DATE"},
        ],
        time_partitioning={
            "field": "dt",
            "type": "DAY",
        },
        exists_ok=True,
    )
    create_final_category_table = BigQueryCreateEmptyTableOperator(
        task_id="create_final_category_table",
        dataset_id=db_name,
        table_id="tmbr_final_category_mapping_table",
        schema_fields=[
            {"name": "brand_name", "type": "STRING"},
            {"name": "meidum_categories", "type": "STRING"},
            {"name": "small_categories", "type": "STRING"},
            {"name": "dt", "type": "DATE"},
        ],
        time_partitioning={
            "field": "dt",
            "type": "DAY",
        },
        exists_ok=True,
    )

    create_tmbr_meta_active_table = BigQueryCreateEmptyTableOperator(
        task_id="create_tmbr_meta_active_table",
        dataset_id=db_name,
        table_id="tmbr_meta_active_tbl",
        schema_fields=[
            {"name": "brand_id", "type": "STRING"},
            {"name": "brand_name", "type": "STRING"},
            {"name": "meidum_categories", "type": "STRING"},
            {"name": "small_categories", "type": "STRING"},
            {"name": "brand_name_model", "type": "STRING"},
            {"name": "description", "type": "STRING"},
            {"name": "updated_at", "type": "STRING"},
            {"name": "created_at", "type": "STRING"},
            {"name": "del_yn", "type": "STRING"},
            {"name": "brand_status_type", "type": "STRING"},
            {"name": "dt", "type": "DATE"},
        ],
        time_partitioning={
            "field": "dt",
            "type": "DAY",
        },
        exists_ok=True,
    )

    tmbr_meta_table = create_nes_task(
        dag=dag,
        task_id="tmbr_meta_table",
        notebook_path=meta_process_path,
        notebook_name="p_tmbr_item_meta.ipynb",
        parameters={"current_dt": "{{ ds }}", "state": env, "ttl": 60},
        doc_md={
            "task_description": "tmbr 메타 테이블 만드는 테스크",
            "output_tables": "tmbr_meta_mapping_tbl, tmbr_meta_active_tbl, tmbr_final_category_mapping_table",
            "reference_tables": "tmbr_operation_tbl, comm.mp_taxonomies_brand, ",
        },
    )

    """DAG CHAIN"""
    (
        start
        >> [
            create_tmbr_etymology_table,
            create_basic_category_table,
            create_final_category_table,
            create_tmbr_meta_active_table,
        ]
        >> tmbr_meta_table
        >> end
    )
