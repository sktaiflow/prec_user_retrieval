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


from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTablePartitionExistenceSensor,
)
from airflow.models.variable import Variable

from airflow.providers.sktvane.operators.nes import NesOperator
from airflow.utils import timezone
from airflow.utils.edgemodifier import Label
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.providers.sktvane.macros.gcp import bigquery_client

local_tz = pendulum.timezone("Asia/Seoul")

### 
conn_id = 'slack_conn'

env = Variable.get("env", "stg")

gcp_project_id = Variable.get("GCP_PROJECT_ID", "skt-datahub")
default_args = {
    "retries": 24,
    "depends_on_past": True,
    'retry_delay': timedelta(hours=1),
}

common_process_notebook_path = "./common/preprocessing/notebook"
log_process_path = f"{common_process_notebook_path}/log"
meta_process_path = f"{common_process_notebook_path}/meta"

class TimePassedSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, target_hour, retry_delay=timedelta(hours=1), *args, **kwargs):
        super(TimePassedSensor, self).__init__(mode='reschedule', *args, **kwargs)
        self.target_hour = target_hour
        self.retry_delay = retry_delay

    def poke(self, context):
        current_time = datetime.now().time()
        target_time = time(hour=self.target_hour)
        if current_time >= target_time:
            return True
        else:
            self.log.info(f"Current time {current_time} is before target time {target_time}. Rescheduling...")
            return False

    def execute(self, context):
        while not self.poke(context):
            self.log.info(f"Task will retry after {self.retry_delay}.")
            self.defer(trigger=TimeDeltaTrigger(self.retry_delay), method_name="execute")



with DAG(
    dag_id=f"CommonPreprocessProfiles_{env}",
    default_args=default_args,
    description="DAG with own plugins",
    #schedule="0 22 * * *",
    schedule_interval='@daily',
    start_date=pendulum.datetime(2024, 7, 2, tz=local_tz),
    catchup=True,
    max_active_runs=1,
    tags=["CommonPreprocessProfiles"],
    
) as dag: 
    
    #start = DummyOperator(task_id='start', dag=dag)
    end_preprocess = DummyOperator(task_id='end_preprocess', dag=dag)

    time_sensor_10pm = TimePassedSensor(
        task_id='wait_until_10pm',
        target_hour=22
    )

    time_sensor_3am = TimePassedSensor(
    task_id='wait_until_3am',
    target_hour=3
    )


    xdr_cat1_cnt =  NesOperator(
        task_id="xdr_cat1_cnt",
        parameters={"current_dt": "{{ ds }}", "state": env, "ttl": "60"},
        input_nb=f"{log_process_path}/p_xdr_cat1.ipynb",
    )

    tmap_item_cnt =  NesOperator(
        task_id="tmap_item_cnt",
        parameters={"current_dt": "{{ ds }}", "state": env, "ttl": "60"},
        input_nb=f"{log_process_path}/p_tmap_item.ipynb",
    )
    tmap_cat1_cnt =  NesOperator(
        task_id="tmap_cat1_cnt",
        parameters={"current_dt": "{{ ds }}", "state": env, "ttl": "60"},
        input_nb=f"{log_process_path}/p_tmap_cat1.ipynb",
    )

    st11_cat1_cnt =  NesOperator(
        task_id="st11_cat1_cnt",
        parameters={"current_dt": "{{ ds }}", "state": env, "ttl": "60"},
        input_nb=f"{log_process_path}/p_11st_cat1.ipynb",
    )

    tdeal_cat1_cnt =  NesOperator(
        task_id="tdeal_cat1_cnt",
        parameters={"current_dt": "{{ ds }}", "state": env, "ttl": "60"},
        input_nb=f"{log_process_path}/p_tdeal_cat1.ipynb",
    )

    adot_cat1_cnt =  NesOperator(
        task_id="adot_cat1_cnt",
        parameters={"current_dt": "{{ macros.ds_add(ds, 2) }}", "state": env, "ttl": "30"},
        input_nb=f"{log_process_path}/p_adot_cat1.ipynb",
    )

    adot_item_cnt =  NesOperator(
        task_id="adot_item_cnt",
        parameters={"current_dt": "{{ macros.ds_add(ds, 2) }}", "state": env, "ttl": "30"},
        input_nb=f"{log_process_path}/p_adot_item.ipynb",
    )

    # tmbr_item_cnt = NesOperator(
    #     task_id="tmbr_item_cnt",
    #     parameters={"current_dt": "{{ ds }}", "state": env, "ttl": "60"},
    #     input_nb=f"{log_process_path}/p_tmbr_item.ipynb",
    # )

    tmbr_meta_table = NesOperator(
        task_id="tmbr_meta_table",
        parameters={"current_dt": "{{ ds }}", "state": env, "ttl": "60"},
        input_nb=f"{meta_process_path}/p_tmbr_item_meta.ipynb",
    )

    """DAG CHAIN"""

    time_sensor_10pm >> [xdr_cat1_cnt, tmap_item_cnt, tmap_cat1_cnt, adot_cat1_cnt, adot_item_cnt, tdeal_cat1_cnt, st11_cat1_cnt] >> end_preprocess
    time_sensor_3am  >> tmbr_meta_table >> end_preprocess
