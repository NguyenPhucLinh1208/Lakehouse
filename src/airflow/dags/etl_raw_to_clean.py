from __future__ import annotations

import pendulum
from docker.types import Mount

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models.param import Param

SPARK_CLIENT_IMAGE_NAME = "my-spark-client:latest"
HOST_SPARK_APPS_DIR = "/home/nplinhwsl/DataEngineerProject/Lakehouse/src/Apache_Spark/apps"
CONTAINER_APP_BASE_PATH = "/app"
APPLICATION_PATH_IN_CONTAINER = f"{CONTAINER_APP_BASE_PATH}/ETL_raw_to_clean.py"
JARS_DIR_IN_CONTAINER = f"{CONTAINER_APP_BASE_PATH}/jars"

JARS_LIST_IN_CONTAINER = [
    f"{JARS_DIR_IN_CONTAINER}/hadoop-aws-3.3.4.jar",
    f"{JARS_DIR_IN_CONTAINER}/aws-java-sdk-bundle-1.12.783.jar",
    f"{JARS_DIR_IN_CONTAINER}/iceberg-spark-runtime-3.5_2.12-1.9.0.jar",
    f"{JARS_DIR_IN_CONTAINER}/nessie-spark-extensions-3.5_2.12-0.103.5.jar"
]
JARS_STRING_FOR_SPARK_SUBMIT = ",".join(JARS_LIST_IN_CONTAINER)

SPARK_SUBMIT_PATH_IN_CLIENT_IMAGE = "/usr/local/bin/spark-submit"

with DAG(
    dag_id="ETL_raw_zone_to_clean_zone",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
    tags=["news", "etl", "spark", "docker_operator"],
    params={
        "etl_start_date": Param(
            default=None,
            type=["null", "string"],
            description="Optional: Start date for ETL (YYYY-MM-DD). If None, uses script's default logic (last 7 days)."
        ),
        "etl_end_date": Param(
            default=None,
            type=["null", "string"],
            description="Optional: End date for ETL (YYYY-MM-DD). If None, uses script's default logic (last 7 days)."
        ),
    },
) as dag:
 
    spark_submit_cmd = f"{SPARK_SUBMIT_PATH_IN_CLIENT_IMAGE} " \
                       f"--master spark://spark-master:7077 " \
                       f"--deploy-mode client " \
                       f"--name airflow_docker_etl_param_{{{{ run_id }}}} " \
                       f"--jars {JARS_STRING_FOR_SPARK_SUBMIT} " \
                       f"--verbose " \
                       f"{APPLICATION_PATH_IN_CONTAINER}"

    # Thêm tham số nếu được cung cấp
    if "{{ params.etl_start_date }}" != "None":
        spark_submit_cmd += f" --etl-start-date {{{{ params.etl_start_date }}}}"
    if "{{ params.etl_end_date }}" != "None":
        spark_submit_cmd += f" --etl-end-date {{{{ params.etl_end_date }}}}"

    submit_spark_job = DockerOperator(
        task_id="submit_etl_raw_to_clean_job",
        image=SPARK_CLIENT_IMAGE_NAME,
        container_name="spark-client_etl_raw_to_clean_job",
        command=['bash', '-c', spark_submit_cmd],
        docker_url="unix://var/run/docker.sock",
        network_mode="lakehouse_net",
        auto_remove='success',
        mounts=[
            Mount(
                source=HOST_SPARK_APPS_DIR,
                target=CONTAINER_APP_BASE_PATH,
                type='bind',
                read_only=False
            )
        ],
        port_bindings={
            "4040": "4048"
        },
        mount_tmp_dir=False,
    )

