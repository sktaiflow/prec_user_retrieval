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

from macros.custom_slack import CallbackNotifier, SlackBot
from macros.custom_nes_task import create_nes_task
from macros.airflow_variables_templates import create_airflow_variables_enum, DEFAULT_VARIABLES


local_tz = pendulum.timezone("Asia/Seoul")

## GET AIRFLOW VARIABLE ###
AirflowVariables = create_airflow_variables_enum(var_dict=DEFAULT_VARIABLES)
print(AirflowVariables)

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
    "profile_pivot_table",
    "end",
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
    end = DummyOperator(task_id="end", dag=dag)

    profile_adot = create_nes_task(
        dag=dag,
        task_id="profile_adot",
        notebook_path=notebook_path,
        notebook_name="profiling_adot.ipynb",
        parameters={"current_dt": "{{ ds }}", "state": env, "log_duration": "60"},
        doc_md={
            "task_description": "adot log를 이용한 User & model profile 생성",
            "output_tables": "adotServiceProfile_adot, adotServiceProfile_templated_adot",
            "reference_tables": "adot_reco_dev.adot_cat1_cnt",
        },
    )

    profile_adot_weekend = create_nes_task(
        dag=dag,
        task_id="profile_adot_weekend",
        notebook_path=notebook_path,
        notebook_name="profiling_adot_weekend.ipynb",
        parameters={"current_dt": "{{ ds }}", "state": env, "log_duration": "60"},
        doc_md={
            "task_description": "adot 주말 log를 이용한 User Profile만 생성 (주말 로그만 사용)",
            "output_tables": "adotServiceProfile_adot_weekend, adotServiceProfile_templated_adot_weekend",
            "reference_tables": "adot_reco_dev.adot_cat1_cnt",
        },
    )

    profile_tdeal = create_nes_task(
        dag=dag,
        task_id="profile_tdeal",
        notebook_path=notebook_path,
        notebook_name="profiling_tdeal.ipynb",
        parameters={"current_dt": "{{ ds }}", "state": env, "log_duration": "60"},
        doc_md={
            "task_description": "tdeal log를 이용한 User & model profile 생성",
            "output_tables": "adotServiceProfile_tdeal, adotServiceProfile_templated_tdeal, adotServiceProfile_templated_tdeal_model",
            "reference_tables": "adot_reco_dev.tdeal_cat1_cnt",
        },
    )

    profile_tmap = create_nes_task(
        dag=dag,
        task_id="profile_tmap",
        notebook_path=notebook_path,
        notebook_name="profiling_tmap.ipynb",
        parameters={"current_dt": "{{ ds }}", "state": env, "log_duration": "60"},
        doc_md={
            "task_description": "tmap log를 이용한 User & model profile 생성",
            "output_tables": "adotServiceProfile_tmap, adotServiceProfile_templated_tmap, adotServiceProfile_templated_tmap_model",
            "reference_tables": "adot_reco_dev.tmap_item_cnt",
        },
    )

    profile_xdr = create_nes_task(
        dag=dag,
        task_id="profile_xdr",
        notebook_path=notebook_path,
        notebook_name="profiling_xdr.ipynb",
        parameters={"current_dt": "{{ ds }}", "state": env, "log_duration": "30"},
        doc_md={
            "task_description": "xdr log를 이용한 User & model profile 생성",
            "output_tables": "adotServiceProfile_xdr, adotServiceProfile_templated_xdr",
            "reference_tables": "adot_reco_dev.xdr_cat1_cnt",
        },
    )

    profile_xdr_weekend = create_nes_task(
        dag=dag,
        task_id="profile_xdr_weekend",
        notebook_path=notebook_path,
        notebook_name="profiling_xdr_weekend.ipynb",
        parameters={"current_dt": "{{ ds }}", "state": env, "log_duration": "60"},
        doc_md={
            "task_description": "xdr 주말 log를 이용한 User profile 생성 (is_weekend=1)",
            "output_tables": "adotServiceProfile_xdr_weekend, adotServiceProfile_templated_xdr_weekend",
            "reference_tables": "adot_reco_dev.xdr_cat1_cnt",
        },
    )
    profile_tmbr = create_nes_task(
        dag=dag,
        task_id="profile_tmbr",
        notebook_path=notebook_path,
        notebook_name="profiling_tmbr.ipynb",
        parameters={"current_dt": "{{ ds }}", "state": env, "log_duration": "60"},
        doc_md={
            "task_description": "tmbr log를 이용한 User profile 생성",
            "output_tables": "adotServiceProfile_tmbr, adotServiceProfile_templated_tmbr",
            "reference_tables": "adot_reco_dev.tmbr_cat1_cnt",
        },
    )

    nudge_offering_table = create_nes_task(
        dag=dag,
        task_id="nudge_offering_table",
        notebook_path=notebook_path,
        notebook_name="nudge_offering_table.ipynb",
        parameters={
            "current_dt": "{{ ds }}",
            "state": env,
            "nudge_token": nudge_api_token,
        },
        doc_md={
            "task_description": "nudge offering api 호출 후 값 저장",
            "output_tables": "nudge_offering_api_table",
            "reference_tables": "",
        },
    )

    create_pivot_table = BigQueryCreateEmptyTableOperator(
        task_id="create_pivot_table",
        dataset_id=db_name,
        table_id="adotServiceMultiProfilesPivotTable",
        schema_fields=[
            {"name": "profile_templates", "type": "STRING"},
            {"name": "source_domain", "type": "STRING"},
            {"name": "profile_id", "type": "INTEGER"},
            {"name": "dt", "type": "DATE"},
        ],
        time_partitioning={
            "field": "dt",
            "type": "DAY",
        },
        exists_ok=True,
    )

    profile_pivot_table = create_nes_task(
        dag=dag,
        task_id="profile_pivot_table",
        notebook_path=notebook_path,
        notebook_name="pivoting_profile.ipynb",
        parameters={"current_dt": "{{ ds }}", "state": env, "ttl": 30},
        doc_md={
            "task_description": "Profile 기준 pivotTable 생성 | User 단위 pivotTable 생성 ",
            "output_tables": "adotServiceMultiProfilesPivotTable, adotServiceUnionUserProfiles",
            "reference_tables": "adotServiceProfile_templated_tmbr, adotServiceProfile_templated_xdr, adotServiceProfile_templated_tdeal, adotServiceProfile_templated_tmap, adotServiceProfile_templated_adot",
        },
    )

    start >> nudge_offering_table >> end
    start >> create_pivot_table
    (
        create_pivot_table
        >> [
            profile_adot,
            profile_tdeal,
            profile_tmap,
            profile_xdr,
            profile_tmbr,
            profile_xdr_weekend,
            profile_adot_weekend,
        ]
        >> profile_pivot_table
        >> end
    )
