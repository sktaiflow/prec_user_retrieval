"""
### DAG Documentation
이 DAG는 HivePartitionSensor를 사용하는 예제입니다.
"""
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
    BigQueryCheckOperator
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

### AIRFLOW VARIABLE ###
local_tz = pendulum.timezone("Asia/Seoul")
conn_id = 'slack_conn'
env = Variable.get("env", "stg")
project_id = Variable.get("GCP_PROJECT_ID", "skt-datahub")
db_name = "adot_reco_dev"



default_args = {
    "retries": 24,
    "depends_on_past": True,
    'retry_delay': timedelta(hours=1),
}

common_process_notebook_path = "./common/preprocessing/notebook"
log_process_path = f"{common_process_notebook_path}/log"
meta_process_path = f"{common_process_notebook_path}/meta"
tmbr_meta_original_table = "mp_taxonomies_brand"


with DAG(
    dag_id=f"CommonMetaPreprocess_{env}",
    default_args=default_args,
    description="DAG with own plugins",
    schedule="0 18 * * *",
    start_date=pendulum.datetime(2024, 7, 2, tz=local_tz),
    catchup=True,
    max_active_runs=1,
    tags=["CommonMetaPreprocess"],
    
) as dag: 

    def check_meta_update(**kwargs):
        hook = BigQueryHook(bigquery_conn_id='bigquery_default', use_legacy_sql=False)
        sql=f"""
            SELECT COUNT(*) as count
            FROM {project_id}.{db_name}.{tmbr_meta_original_table}
            WHERE updated_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) OR created_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
            AND your_condition
        """
        result = hook.get_first(sql)
        if result and result[0] > 0:
            return 'tmbr_meta_table'
        else:
            return 'end'

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id='end')

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=db_name,
        table_id="tmbr_meta_mapping_tbl",
        schema_fields=[
            {"name": "brand_id", "type": "STRING"},
            {"name": "brand_name", "type": "STRING"},
            {"name": "categories", "type": "STRING"},
            {"name": "brand_name_model", "type": "STRING"},
            {"name": "updated_at", "type": "STRING"},
            {"name": "created_at", "type": "STRING"},
            {"name": "profile_list", "type": "STRING"},
            {"name": "dt", "type": "DATE"},
        ],
        time_partitioning={
            "field": "dt",
            "type": "DAY",
            "expirationMs": 30 * 24 * 60 * 60 * 1000,
        },
        exists_ok=True,
    )
    check_meta_update_branch = BranchPythonOperator(
        task_id='check_meta_update_branch',
        python_callable=check_meta_update
    )
    
    tmbr_meta_table = NesOperator(
        task_id="tmbr_meta_table",
        parameters={"current_dt": "{{ ds }}", "state": env, "ttl": "60"},
        input_nb=f"{meta_process_path}/p_tmbr_item_meta.ipynb",
    )

    """DAG CHAIN"""

    start >> create_table >> check_meta_update_branch >> [end, tmbr_meta_table]
    tmbr_meta_table >> end