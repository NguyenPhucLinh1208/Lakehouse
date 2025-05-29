# ~/DataEngineerProject/Lakehouse/src/airflow/dags/news_etl_dag_run_original_script.py

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# --- Đường dẫn tới script PySpark và JARs BÊN TRONG IMAGE AIRFLOW WORKER ---
# (Dựa trên thông tin ls bạn cung cấp)
APPLICATION_PATH = "/opt/project_code/Apache_Spark/apps/ETL_raw_to_clean.py"

JARS_DIR_PATH = "/opt/project_code/Apache_Spark/apps/jars"
JARS_LIST = [
    f"{JARS_DIR_PATH}/hadoop-aws-3.3.4.jar",
    f"{JARS_DIR_PATH}/aws-java-sdk-bundle-1.12.783.jar",
    f"{JARS_DIR_PATH}/iceberg-spark-runtime-3.5_2.12-1.9.0.jar",
    f"{JARS_DIR_PATH}/nessie-spark-extensions-3.5_2.12-0.103.5.jar"
]
JARS_STRING = ",".join(JARS_LIST)

# Tên Airflow Connection cho Spark bạn đã tạo
SPARK_CONNECTION_ID = "spark_cluster_conn"

with DAG(
    dag_id="news_etl_run_original_script",
    # start_date nên là một ngày cố định trong quá khứ nếu bạn không muốn catchup
    
    start_date=pendulum.datetime(2025, 5, 1, tz="UTC"), # Hoặc ngày bạn muốn bắt đầu theo dõi
    schedule=None,  # Đặt là None để chỉ chạy thủ công ban đầu, hoặc "@once"
    catchup=False,  # Không chạy bù các kỳ đã qua
    doc_md="""
    ### ETL Pipeline: Run Original Raw News to Clean News Script
    
    This DAG orchestrates the **original** Spark job (`ETL_raw_to_clean.py`)
    to transform raw news data.
    
    **Important Notes for this version:**
    - The Spark script uses its **hardcoded dates** for ETL processing.
    - The Spark script uses its **hardcoded MinIO and Nessie configurations**.
    - The Airflow MinIO connection `my_lakehouse_conn` is not actively used by this DAG
      to pass credentials to the Spark job in this iteration.
    """,
    tags=["news", "etl", "spark", "initial_run"],
) as dag:
    submit_original_spark_etl_job = SparkSubmitOperator(
        task_id="submit_original_news_etl_job",
        application=APPLICATION_PATH,       # Đường dẫn đến script PySpark
        conn_id=SPARK_CONNECTION_ID,        # Sử dụng connection 'spark_cluster_conn'
        jars=JARS_STRING,                   # Danh sách các JARs
        # application_args=[],              # Không truyền args, script tự dùng giá trị hardcode
        # conf={},                          # Không truyền conf, script tự cấu hình SparkSession
        verbose=True,                       # In log chi tiết của spark-submit
    )
