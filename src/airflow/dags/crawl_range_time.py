from __future__ import annotations
import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException
from Web_scraping.CrawlJob import process_date_range_and_categories
from common.datasets import raw_vnexpress_data_ready

DAG_ID = "vnexpress_daily_crawl_range_day_dag"
MINIO_CONNECTION_ID = "my_lakehouse_conn"
MINIO_BUCKET_NAME = "raw-news-lakehouse"
SELENIUM_HUB_URL = "http://selenium-hub:4444/wd/hub"
SELENIUM_POOL_NAME = "selenium_pool"

DEFAULT_ARGS = {
    'owner': 'airflow_user',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=1),
}

@dag(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='DAG crawl VnExpress hàng ngày (7 ngày gần nhất), và có thể chạy thủ công theo khoảng ngày.',
    schedule='0 1 * * *',
    catchup=False,
    tags=['web_scraping', 'vnexpress', 'daily_crawl', 'producer'],
    params={
        "pipeline_start_date": Param(
            default=pendulum.now("Asia/Ho_Chi_Minh").subtract(days=7).format("YYYYMMDD"),
            type="string", title="Ngày Bắt Đầu (chỉ cho manual run)",
        ),
        "pipeline_end_date": Param(
            default=pendulum.now("Asia/Ho_Chi_Minh").subtract(days=1).format("YYYYMMDD"),
            type="string", title="Ngày Kết Thúc (chỉ cho manual run)",
        )
    }
)
def vnexpress_daily_crawl_dag():

    @task(task_id="generate_list_of_dates")
    def generate_single_dates_to_process(**context) -> list[dict[str, str]]:
        task_logger = logging.getLogger("airflow.task")
        dag_run = context["dag_run"]
        dates_to_process_list = []

        try:
            if dag_run.run_type == "scheduled":
                task_logger.info("[Scheduled Run] Sẽ xử lý cho 7 ngày gần nhất.")
                end_date = context["data_interval_end"].in_timezone("Asia/Ho_Chi_Minh").subtract(days=1)
                start_date = end_date.subtract(days=6)
                task_logger.info(f"Khoảng ngày tự động: {start_date.format('YYYYMMDD')} đến {end_date.format('YYYYMMDD')}")
                current_date = start_date
                while current_date <= end_date:
                    dates_to_process_list.append({"processing_date_str": current_date.format("YYYYMMDD")})
                    current_date = current_date.add(days=1)
                return dates_to_process_list
            else:
                task_logger.info("[Manual Run] Sẽ xử lý theo khoảng ngày từ params.")
                params = context["params"]
                start_str = params.get("pipeline_start_date")
                end_str = params.get("pipeline_end_date")
                current_date = pendulum.from_format(start_str, "YYYYMMDD", tz="Asia/Ho_Chi_Minh")
                final_date = pendulum.from_format(end_str, "YYYYMMDD", tz="Asia/Ho_Chi_Minh")
                while current_date <= final_date:
                    dates_to_process_list.append({"processing_date_str": current_date.format("YYYYMMDD")})
                    current_date = current_date.add(days=1)
                return dates_to_process_list
        except Exception as e:
            task_logger.error(f"Lỗi khi tạo danh sách ngày: {e}", exc_info=True)
            return []

    @task(task_id="crawl_and_upload_single_day", pool=SELENIUM_POOL_NAME)
    def crawl_and_upload_for_single_day_task(date_to_process_info: dict[str, str]) -> str | None:
        processing_date = date_to_process_info["processing_date_str"]
        task_logger = logging.getLogger("airflow.task")
        try:
            s3_hook_minio = S3Hook(aws_conn_id=MINIO_CONNECTION_ID)
            process_date_range_and_categories(
                selenium_hub_url=SELENIUM_HUB_URL,
                start_date_str=processing_date,
                end_date_str=processing_date,
                s3_hook_for_minio=s3_hook_minio,
                minio_bucket_name=MINIO_BUCKET_NAME
            )
            task_logger.info(f"Hoàn thành crawl cho ngày: {processing_date}")
            return processing_date
        except Exception as e:
            task_logger.error(f"Lỗi crawl ngày {processing_date}: {e}", exc_info=True)
            return None

    @task(task_id="summarize_and_produce_signal", outlets=[raw_vnexpress_data_ready])
    def summarize_results_and_produce_signal(processed_dates_results: list[str | None], **context):
        task_logger = logging.getLogger("airflow.task")
        dag_run = context["dag_run"]
        
        if dag_run.run_type == "manual":
            task_logger.info("Manual run detected. Skipping dataset production to avoid triggering downstream DAGs.")
            raise AirflowSkipException

        successful_dates = [d for d in processed_dates_results if d]
        failed_count = len(processed_dates_results) - len(successful_dates)
        task_logger.info(f"Tổng kết: {len(successful_dates)} thành công, {failed_count} thất bại.")
        
        if not successful_dates:
            raise ValueError("Không có dữ liệu nào được crawl thành công, sẽ không trigger ETL.")
        
        task_logger.info(f"Đã crawl thành công. Dataset '{raw_vnexpress_data_ready.uri}' sẽ được cập nhật.")

    list_of_dates_to_crawl = generate_single_dates_to_process()
    crawl_results = crawl_and_upload_for_single_day_task.expand(
        date_to_process_info=list_of_dates_to_crawl
    )
    summarize_results_and_produce_signal(processed_dates_results=crawl_results)

vnexpress_daily_crawl_dag()
