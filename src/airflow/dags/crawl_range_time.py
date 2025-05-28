from __future__ import annotations

import logging
import pendulum

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from Web_scraping.CrawlJob import process_date_range_and_categories

DAG_ID = "vnexpress_crawl_range_time_dag"
MINIO_CONNECTION_ID = "my_lakehouse_conn"
MINIO_BUCKET_NAME = "raw-news-lakehouse"
SELENIUM_HUB_URL = "http://selenium-hub:4444/wd/hub"
SELENIUM_POOL_NAME = "selenium_pool"

DEFAULT_START_DATE = pendulum.now("Asia/Ho_Chi_Minh").subtract(days=7).format("YYYYMMDD")
DEFAULT_END_DATE = pendulum.now("Asia/Ho_Chi_Minh").subtract(days=1).format("YYYYMMDD")

DEFAULT_ARGS = {
    'owner': 'airflow_user',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False,
}

DAG_DOC_MD = f"""
### DAG Thu Thập Dữ Liệu Lịch Sử VnExpress Song Song Theo Ngày (Có Thể Cấu Hình Ngày)

**Mục đích:**
- Cho phép người dùng chỉ định khoảng ngày (Từ Ngày - Đến Ngày) cần thu thập khi trigger DAG.
- Tự động tạo task cho mỗi ngày trong khoảng thời gian được chỉ định.
- Thực hiện thu thập dữ liệu (crawl) từ VnExpress cho từng ngày một cách song song, được giới hạn bởi Airflow Pool.
- Tải dữ liệu đã thu thập lên MinIO S3.

**Luồng hoạt động:**
1.  **`generate_single_dates_to_process`**: Tạo danh sách các ngày đơn lẻ (định dạng YYYYMMDD) cần xử lý dựa trên `pipeline_start_date` và `pipeline_end_date` từ params.
2.  **`crawl_and_upload_for_single_day_task` (dynamic tasks)**: Với mỗi ngày được tạo, một task song song sẽ được tạo để:
    * Gọi hàm `process_date_range_and_categories` để thu thập dữ liệu.
    * Sử dụng `S3Hook` để tương tác với MinIO.
    * Các task này được chạy trong pool `{SELENIUM_POOL_NAME}`.
3.  Nếu không có ngày nào hợp lệ, các task crawl sẽ được bỏ qua.

**Tham số khi Trigger DAG:**
- `pipeline_start_date` (YYYYMMDD): Ngày bắt đầu thu thập dữ liệu. Mặc định: {DEFAULT_START_DATE}.
- `pipeline_end_date` (YYYYMMDD): Ngày kết thúc thu thập dữ liệu. Mặc định: {DEFAULT_END_DATE}.

Số task Selenium song song tối đa được quản lý bởi `{SELENIUM_POOL_NAME}`.
"""

@dag(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='DAG crawl VnExpress song song theo ngày, ngày có thể cấu hình, giới hạn bởi pool.',
    schedule=None,
    catchup=False,
    tags=['web_scraping', 'vnexpress', 'minio', 'history_crawl', 'configurable_date'],
    doc_md=DAG_DOC_MD,
    params={
        "pipeline_start_date": Param(
            default=DEFAULT_START_DATE,
            type="string",
            title="Ngày Bắt Đầu Pipeline (YYYYMMDD)",
            description=f"Ngày bắt đầu (YYYYMMDD) để thu thập dữ liệu lịch sử. Mặc định là 7 ngày trước: {DEFAULT_START_DATE}.",
        ),
        "pipeline_end_date": Param(
            default=DEFAULT_END_DATE,
            type="string",
            title="Ngày Kết Thúc Pipeline (YYYYMMDD)",
            description=f"Ngày kết thúc (YYYYMMDD) để thu thập dữ liệu lịch sử. Mặc định là ngày hôm qua: {DEFAULT_END_DATE}.",
        )
    }
)
def vnexpress_crawl_range_time_dag(): 

    @task(task_id="generate_list_of_dates")
    def generate_single_dates_to_process(
        params: dict
    ) -> list[dict[str, str]]:
        dates_to_process_list = []
        task_logger = logging.getLogger("airflow.task")

        global_start_date_str = params.get("pipeline_start_date")
        global_end_date_str = params.get("pipeline_end_date")

        if not global_start_date_str or not global_end_date_str:
            task_logger.error("Thiếu tham số pipeline_start_date hoặc pipeline_end_date trong params của DAG run.")
            return []

        try:
            current_date = pendulum.from_format(global_start_date_str, "YYYYMMDD", tz="Asia/Ho_Chi_Minh")
            final_date = pendulum.from_format(global_end_date_str, "YYYYMMDD", tz="Asia/Ho_Chi_Minh")

            if current_date > final_date:
                task_logger.error(f"Ngày bắt đầu ({global_start_date_str}) không thể lớn hơn ngày kết thúc ({global_end_date_str}).")
                return []

            task_logger.info(
                f"Sẽ tạo danh sách các ngày từ {global_start_date_str} đến {global_end_date_str} (từ params)."
            )
            while current_date <= final_date:
                dates_to_process_list.append(
                    {"processing_date_str": current_date.format("YYYYMMDD")}
                )
                current_date = current_date.add(days=1)
            
            if not dates_to_process_list:
                task_logger.info("Không có ngày nào được tạo để xử lý dựa trên params.")
            else:
                task_logger.info(f"Đã tạo {len(dates_to_process_list)} ngày để xử lý: {dates_to_process_list[:3]}... (ví dụ)")

        except ValueError as ve:
            task_logger.error(f"Lỗi định dạng ngày đầu vào từ params: {ve}", exc_info=True)
            return []
        except Exception as e:
            task_logger.error(f"Lỗi không xác định khi tạo danh sách ngày từ params: {e}", exc_info=True)
            return []
        
        return dates_to_process_list

    @task(task_id="crawl_and_upload_single_day", pool=SELENIUM_POOL_NAME)
    def crawl_and_upload_for_single_day_task(
        date_to_process_info: dict[str, str],
        selenium_hub_url: str,
        s3_conn_id: str,
        s3_bucket_name: str
    ) -> str | None:
        processing_date = date_to_process_info["processing_date_str"]
        log_prefix = f"[Crawl Day: {processing_date}]"
        task_logger = logging.getLogger("airflow.task")
        
        task_logger.info(f"{log_prefix} Bắt đầu quá trình crawl trên Selenium Hub: {selenium_hub_url}")
        task_logger.info(f"{log_prefix} Sử dụng S3 connection ID: {s3_conn_id} cho bucket: {s3_bucket_name}")

        try:
            s3_hook_minio = S3Hook(aws_conn_id=s3_conn_id)
            task_logger.info(f"{log_prefix} Đã khởi tạo S3Hook thành công.")

            process_date_range_and_categories(
                selenium_hub_url=selenium_hub_url,
                start_date_str=processing_date,
                end_date_str=processing_date,
                s3_hook_for_minio=s3_hook_minio,
                minio_bucket_name=s3_bucket_name
            )
            task_logger.info(f"{log_prefix} Hàm process_date_range_and_categories đã hoàn thành.")
            return processing_date

        except Exception as e:
            task_logger.error(f"{log_prefix} Lỗi trong quá trình crawl và upload: {e}", exc_info=True)
            return None

    @task(task_id="summarize_daily_crawl_results")
    def summarize_results(processed_dates_results: list[str | None]):
        task_logger = logging.getLogger("airflow.task")
        successful_dates = [date_str for date_str in processed_dates_results if date_str is not None]
        failed_count = processed_dates_results.count(None)

        task_logger.info("----- TỔNG KẾT QUÁ TRÌNH THU THẬP DỮ LIỆU LỊCH SỬ THEO NGÀY -----")
        if successful_dates:
            task_logger.info(f"Số ngày được xử lý và crawl thành công: {len(successful_dates)}")
            task_logger.info(f"Các ngày thành công (ví dụ): {successful_dates[:5]}")
        else:
            task_logger.info("Không có ngày nào được xử lý thành công trong lần chạy này.")
        
        if failed_count > 0:
            task_logger.warning(f"Số lượng task xử lý ngày thất bại: {failed_count}")
        
        return {
            "total_tasks_expanded": len(processed_dates_results),
            "successful_dates_count": len(successful_dates),
            "failed_tasks_count": failed_count,
            "example_successful_dates": successful_dates[:20]
        }

    start_pipeline = EmptyOperator(task_id='Bắt_đầu_pipeline_lịch_sử_configurable')
    end_pipeline = EmptyOperator(task_id='Kết_thúc_pipeline_lịch_sử_configurable')
    
    list_of_dates_to_crawl = generate_single_dates_to_process() 
    
    crawl_results = crawl_and_upload_for_single_day_task.partial(
        selenium_hub_url=SELENIUM_HUB_URL,
        s3_conn_id=MINIO_CONNECTION_ID,
        s3_bucket_name=MINIO_BUCKET_NAME,
    ).expand(date_to_process_info=list_of_dates_to_crawl)
    
    summary = summarize_results(processed_dates_results=crawl_results)
    
    start_pipeline >> list_of_dates_to_crawl >> crawl_results >> summary >> end_pipeline
    
vnexpress_range_time = vnexpress_crawl_range_time_dag()
