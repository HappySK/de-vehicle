#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example DAG demonstrating the usage of the classic Python operators to execute Python functions natively and
within a virtual environment.
"""
from __future__ import annotations

import logging
import sys

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

for api in ('GetAllMakes', 'GetAllManufacturers', 'GetManufacturerDetails'):
    with DAG(
            dag_id=api,
            schedule=None,
            start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
            catchup=False,
            tags=["vehicle"],
    ):
        pipeline_start = EmptyOperator(task_id='pipeline_start')
        get_vehicle_data = BashOperator(
            task_id="get_vehicle_data", bash_command=f'python3 /home/airflow_user/airflow/spark_jobs/get_vehicle_data.py {api}'
        )
        load_vehicle_data = SparkSubmitOperator(
            conn_id='spark-connection', application="airflow/spark_jobs/load_vehicle_data.py",
            task_id="load_vehicle_data",
            jars='/home/airflow_user/airflow/spark/jar/mysql.jar',
            application_args=[api]
        )
        pipeline_end = EmptyOperator(task_id='pipeline_end')

    pipeline_start >> get_vehicle_data >> load_vehicle_data >> pipeline_end
