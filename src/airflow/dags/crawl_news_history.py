from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pendulum

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from Web_scraping.CrawlJob import process_date_range_and_categories

DAG_ID = "vnexpress_dynamic_parallel_crawl_history_data_dag" # Đã bỏ _v2
MINIO_CONNECTION_ID = "my_lakehouse_conn"
MINIO_BUCKET_NAME = "raw-news-lakehouse"
SELENIUM_HUB_URL = "http://selenium-hub:4444/wd/hub"

PIPELINE_GLOBAL_START_DATE_STR = "20240301"
PIPELINE_GLOBAL_END_DATE_STR = "20240630"
NUMBER_OF_PARALLEL_TASKS_CONFIG = 3

DEFAULT_ARGS = {
    'owner': 'airflow_user',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

DAG_DOC_MD = """
### DAG Thu Thập Dữ Liệu VnExpress Song Song

**Mục đích:**
- Tự động tính toán và phân chia khoảng ngày cần thu thập.
- Thực hiện thu thập dữ liệu (crawl) từ VnExpress cho từng khoảng ngày một cách song song.
- Tải dữ liệu đã thu thập lên MinIO S3.

**Luồng hoạt động:**
1.  **`calculate_date_intervals`**: Tính toán các khoảng ngày (start_date, end_date) dựa trên cấu hình đầu vào.
2.  **`crawl_and_upload_for_interval` (dynamic tasks)**: Với mỗi khoảng ngày được tính toán, một task song song sẽ được tạo để:
    * Gọi hàm `process_date_range_and_categories` để thu thập dữ liệu.
    * Sử dụng `S3Hook` để tương tác với MinIO.
3.  Nếu không có khoảng ngày nào hợp lệ, các task crawl sẽ được bỏ qua.

**Cấu hình chính:**
- `PIPELINE_GLOBAL_START_DATE_STR`: {start_date_cfg}
- `PIPELINE_GLOBAL_END_DATE_STR`: {end_date_cfg}
- `NUMBER_OF_PARALLEL_TASKS_CONFIG`: {num_tasks_cfg}
""".format(
    start_date_cfg=PIPELINE_GLOBAL_START_DATE_STR,
    end_date_cfg=PIPELINE_GLOBAL_END_DATE_STR,
    num_tasks_cfg=NUMBER_OF_PARALLEL_TASKS_CONFIG,
)

@dag(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='DAG crawl VnExpress song song, tự động chia ngày, tải lên MinIO (TaskFlow)', # Đã bỏ (v2)
    schedule=None,
    catchup=False,
    tags=['web_scraping', 'vnexpress', 'minio', 'dynamic_parallel', 'taskflow'],
    doc_md=DAG_DOC_MD,
)
def vnexpress_dynamic_parallel_crawl_dag(): # Đã bỏ _v2
    """
    DAG chính điều phối việc tính toán khoảng ngày và thực hiện crawl song song.
    """

    @task(task_id="calculate_date_intervals")
    def calculate_date_intervals(
        global_start_date_str: str,
        global_end_date_str: str,
        num_parallel_tasks_config: int
    ) -> list[dict[str, str]]:
        """
        Tính toán và phân chia khoảng ngày tổng thể thành các khoảng nhỏ hơn
        cho các task chạy song song.
        Trả về danh sách các dictionary, mỗi dict chứa 'start_date' và 'end_date'.
        """
        date_intervals = []
        logger = logging.getLogger("airflow.task")

        try:
            global_start_date = datetime.strptime(global_start_date_str, "%Y%m%d")
            global_end_date = datetime.strptime(global_end_date_str, "%Y%m%d")

            if global_start_date > global_end_date:
                raise ValueError("Ngày bắt đầu không thể lớn hơn ngày kết thúc.")

            total_days_to_process = (global_end_date - global_start_date).days + 1
            logger.info(
                f"Tổng số ngày cần xử lý: {total_days_to_process} "
                f"(từ {global_start_date_str} đến {global_end_date_str})"
            )
            logger.info(f"Số luồng song song dự kiến: {num_parallel_tasks_config}")

            actual_number_of_tasks: int
            if total_days_to_process < num_parallel_tasks_config and total_days_to_process > 0:
                logger.warning(
                    f"Tổng số ngày ({total_days_to_process}) ít hơn số luồng song song "
                    f"({num_parallel_tasks_config}). Sẽ chỉ tạo ra {total_days_to_process} luồng, mỗi luồng 1 ngày."
                )
                actual_number_of_tasks = total_days_to_process
            elif total_days_to_process <= 0:
                logger.warning(f"Không có ngày nào để xử lý (tổng số ngày: {total_days_to_process}).")
                actual_number_of_tasks = 0
            else:
                actual_number_of_tasks = num_parallel_tasks_config

            if actual_number_of_tasks > 0:
                base_days_per_task = total_days_to_process // actual_number_of_tasks
                remaining_days = total_days_to_process % actual_number_of_tasks
                current_task_start_date = global_start_date

                for i in range(actual_number_of_tasks):
                    days_for_this_task = base_days_per_task
                    if remaining_days > 0:
                        days_for_this_task += 1
                        remaining_days -= 1
                    
                    if current_task_start_date + timedelta(days=days_for_this_task - 1) > global_end_date:
                        days_for_this_task = (global_end_date - current_task_start_date).days + 1

                    if days_for_this_task <= 0:
                        continue

                    current_task_end_date = current_task_start_date + timedelta(days=days_for_this_task - 1)
                    
                    start_date_str_for_task = current_task_start_date.strftime("%Y%m%d")
                    end_date_str_for_task = current_task_end_date.strftime("%Y%m%d")

                    date_intervals.append({
                        "start_date_str": start_date_str_for_task,
                        "end_date_str": end_date_str_for_task
                    })
                    logger.info(
                        f"  Khoảng ngày cho Task {i+1}: {days_for_this_task} ngày - "
                        f"Từ {start_date_str_for_task} đến {end_date_str_for_task}"
                    )

                    current_task_start_date = current_task_end_date + timedelta(days=1)
                    if current_task_start_date > global_end_date:
                        break
            
            if not date_intervals and total_days_to_process > 0:
                logger.warning(
                    "Fallback: Không có khoảng ngày nào được tự động phân chia, "
                    "tạo một task duy nhất cho toàn bộ khoảng thời gian."
                )
                date_intervals.append({
                    "start_date_str": global_start_date_str,
                    "end_date_str": global_end_date_str
                })
            elif not date_intervals and total_days_to_process <= 0:
                logger.info("Không có ngày nào để xử lý, sẽ không tạo task crawl nào.")

        except ValueError as ve:
            logger.error(f"Lỗi khi xử lý ngày đầu vào: {ve}", exc_info=True)
            return [] 
        except Exception as e:
            logger.error(f"Lỗi không xác định khi tính toán khoảng ngày: {e}", exc_info=True)
            return []

        logger.info(f"Các khoảng ngày cuối cùng được phân chia cho các task: {date_intervals}")
        if not date_intervals:
            logger.info("Không có khoảng ngày nào được tạo, các task crawl sẽ được bỏ qua.")
        return date_intervals

    @task(task_id="crawl_and_upload_for_interval")
    def crawl_and_upload_task(
        date_interval: dict[str, str],
        selenium_hub_url: str,
        s3_conn_id: str,
        s3_bucket_name: str
    ):
        """
        Thực hiện crawl dữ liệu cho một khoảng ngày cụ thể và tải lên S3/MinIO.
        """
        start_date_to_process = date_interval["start_date_str"]
        end_date_to_process = date_interval["end_date_str"]
        log_prefix = f"[Crawl Interval: {start_date_to_process}-{end_date_to_process}]"
        logger = logging.getLogger("airflow.task")

        logger.info(f"{log_prefix} Bắt đầu quá trình crawl trên Selenium Hub: {selenium_hub_url}")
        logger.info(f"{log_prefix} Sử dụng S3 connection ID: {s3_conn_id} cho bucket: {s3_bucket_name}")

        try:
            s3_hook_minio = S3Hook(aws_conn_id=s3_conn_id)
            logger.info(f"{log_prefix} Đã khởi tạo S3Hook thành công.")

            process_date_range_and_categories(
                selenium_hub_url=selenium_hub_url,
                start_date_str=start_date_to_process,
                end_date_str=end_date_to_process,
                s3_hook_for_minio=s3_hook_minio,
                minio_bucket_name=s3_bucket_name
            )
            logger.info(f"{log_prefix} Hàm process_date_range_and_categories đã hoàn thành.")

        except Exception as e:
            logger.error(f"{log_prefix} Lỗi trong quá trình crawl và upload: {e}", exc_info=True)
            raise

        logger.info(f"{log_prefix} Task crawl và upload hoàn tất thành công!")

    start_pipeline = EmptyOperator(task_id='start_pipeline_processing')
    end_pipeline = EmptyOperator(task_id='end_pipeline_processing')

    calculated_intervals = calculate_date_intervals(
        global_start_date_str=PIPELINE_GLOBAL_START_DATE_STR,
        global_end_date_str=PIPELINE_GLOBAL_END_DATE_STR,
        num_parallel_tasks_config=NUMBER_OF_PARALLEL_TASKS_CONFIG
    )

    crawl_tasks_expanded = crawl_and_upload_task.partial(
        selenium_hub_url=SELENIUM_HUB_URL,
        s3_conn_id=MINIO_CONNECTION_ID,
        s3_bucket_name=MINIO_BUCKET_NAME,
    ).expand(date_interval=calculated_intervals)
    
    if crawl_tasks_expanded:
        start_pipeline >> calculated_intervals >> crawl_tasks_expanded >> end_pipeline
    else:
        start_pipeline >> calculated_intervals >> end_pipeline

# Khởi tạo DAG instance để Airflow có thể nhận diện
vnexpress_crawl_dag_instance = vnexpress_dynamic_parallel_crawl_dag() # Đã bỏ _v2
