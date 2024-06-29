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

local_tz = pendulum.timezone("Asia/Seoul")

### 
conn_id = 'slack_conn'

env = Variable.get("env", "stg")
gcp_project_id = Variable.get("GCP_PROJECT_ID", "skt-datahub")
if env=='prd':
    aidp_db_name = "adot_reco"
else:
    aidp_db_name = "adot_reco_dev"

default_args = {
    "retries": 100,
    "depends_on_past": True
}


with DAG(
    dag_id=f"CommonPreprocessProfiles_{env}",
    default_args=default_args,
    description="DAG with own plugins",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2024, 6, 21, tz=local_tz),
    catchup=True,
    max_active_runs=1,
    tags=["CommonPreprocessProfiles"],
    
) as dag: 
    
    start = DummyOperator(task_id='start', dag=dag)
    end_preprocess = DummyOperator(task_id='end_preprocess', dag=dag)

    ## user retrieval 용 
    xdr_cat1_cnt =  NesOperator(
        task_id="xdr_cat1_cnt",
        parameters={"current_dt": "{{ ds }}", "state": "prd", "duration": "30"},
        input_nb="./preprocessing/notebook/xdr_cat1_cnt.ipynb",
    )
    xdr_cat2_cnt =  NesOperator(
        task_id="xdr_cat2_cnt",
        parameters={"current_dt": "{{ ds }}", "state": "prd", "duration": "30"},
        input_nb="./preprocessing/notebook/xdr_cat2_cnt.ipynb",
    )

    tmap_item_cnt =  NesOperator(
        task_id="tmap_item_cnt",
        parameters={"current_dt": "{{ ds }}", "state": "prd", "duration": "30"},
        input_nb="./preprocessing/notebook/tmap_item_cnt.ipynb",
    )
    tmap_cat1_cnt =  NesOperator(
        task_id="tmap_cat1_cnt",
        parameters={"current_dt": "{{ ds }}", "state": "prd", "duration": "30"},
        input_nb="./preprocessing/notebook/tmap_cat1_cnt.ipynb",
    )
    tmap_cat2_cnt =  NesOperator(
        task_id="tmap_cat2_cnt",
        parameters={"current_dt": "{{ ds }}", "state": "prd", "duration": "30"},
        input_nb="./preprocessing/notebook/tmap_cat2_cnt.ipynb",
    )

    st11_cat1_cnt =  NesOperator(
        task_id="st11_cat1_cnt",
        parameters={"current_dt": "{{ ds }}", "state": "prd", "duration": "30"},
        input_nb="./preprocessing/notebook/st11_cat1_cnt.ipynb",
    )
    st11_cat2_cnt =  NesOperator(
        task_id="st11_cat2_cnt",
        parameters={"current_dt": "{{ ds }}", "state": "prd", "duration": "30"},
        input_nb="./preprocessing/notebook/st11_cat2_cnt.ipynb",
    )

    tdeal_cat1_cnt =  NesOperator(
        task_id="tdeal_cat1_cnt",
        parameters={"current_dt": "{{ ds }}", "state": "prd", "duration": "30"},
        input_nb="./preprocessing/notebook/tdeal_cat1_cnt.ipynb",
    )
    tdeal_cat2_cnt =  NesOperator(
        task_id="tdeal_cat2_cnt",
        parameters={"current_dt": "{{ ds }}", "state": "prd", "duration": "30"},
        input_nb="./preprocessing/notebook/tdeal_cat2_cnt.ipynb",
    )

    adot_cat1_cnt =  NesOperator(
        task_id="adot_cat1_cnt",
        parameters={"current_dt": "{{ macros.ds_add(ds, 2) }}", "state": "prd", "duration": "30"},
        input_nb="./preprocessing/notebook/adot_cat1_cnt.ipynb",
    )
    adot_cat2_cnt =  NesOperator(
        task_id="adot_cat2_cnt",
        parameters={"current_dt": "{{ macros.ds_add(ds, 2) }}", "state": "prd", "duration": "30"},
        input_nb="./preprocessing/notebook/adot_cat2_cnt.ipynb",
    )
    adot_item_cnt =  NesOperator(
        task_id="adot_item_cnt",
        parameters={"current_dt": "{{ macros.ds_add(ds, 2) }}", "state": "prd", "duration": "30"},
        input_nb="./preprocessing/notebook/adot_item_cnt.ipynb",
    )
    """DAG CHAIN"""

    start >> [xdr_cat1_cnt, xdr_cat2_cnt, tmap_item_cnt, tmap_cat1_cnt, tmap_cat2_cnt, st11_cat2_cnt, st11_cat2_cnt, adot_cat1_cnt, adot_cat2_cnt, adot_item_cnt, tdeal_cat1_cnt, tdeal_cat2_cnt] >> end_preprocess