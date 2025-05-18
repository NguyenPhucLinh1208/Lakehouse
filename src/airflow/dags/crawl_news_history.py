from datetime import timedelta, datetime
import logging
import pendulum # Đảm bảo pendulum được import

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.utils.dates import days_ago # <- LOẠI BỎ DÒNG NÀY

# Sử dụng import thực tế từ project của bạn
from Web_scraping.CrawlJob import process_date_range_and_categories

DAG_ID = "vnexpress_dynamic_parallel_crawl_dag"
MINIO_CONNECTION_ID = "my_lakehouse_conn"
MINIO_BUCKET_NAME = "raw-news-lakehouse"
SELENIUM_HUB_URL = "http://selenium-hub:4444/wd/hub"

# --- BEGIN: Cấu hình đầu vào cho việc phân chia ngày ---
PIPELINE_GLOBAL_START_DATE_STR = "20240101"
PIPELINE_GLOBAL_END_DATE_STR = "20240103" 
                                         
NUMBER_OF_PARALLEL_TASKS = 3
# --- END: Cấu hình đầu vào ---

# --- BEGIN: Logic tính toán và phân chia khoảng ngày ---
DATE_INTERVALS_FOR_TASKS = []

try:
    global_start_date = datetime.strptime(PIPELINE_GLOBAL_START_DATE_STR, "%Y%m%d")
    global_end_date = datetime.strptime(PIPELINE_GLOBAL_END_DATE_STR, "%Y%m%d")

    if global_start_date > global_end_date:
        raise ValueError("Ngày bắt đầu không thể lớn hơn ngày kết thúc.")

    total_days_to_process = (global_end_date - global_start_date).days + 1
    logging.info(f"Tổng số ngày cần xử lý: {total_days_to_process} (từ {PIPELINE_GLOBAL_START_DATE_STR} đến {PIPELINE_GLOBAL_END_DATE_STR})")
    logging.info(f"Số luồng song song dự kiến: {NUMBER_OF_PARALLEL_TASKS}")

    if total_days_to_process < NUMBER_OF_PARALLEL_TASKS and total_days_to_process > 0:
        logging.warning(f"Tổng số ngày ({total_days_to_process}) ít hơn số luồng song song ({NUMBER_OF_PARALLEL_TASKS}). "
                        f"Sẽ chỉ tạo ra {total_days_to_process} luồng, mỗi luồng 1 ngày.")
        actual_number_of_tasks = total_days_to_process
    elif total_days_to_process <= 0: # Nếu không có ngày nào hoặc ngày bắt đầu > ngày kết thúc (đã check ở trên nhưng để an toàn)
        logging.warning(f"Không có ngày nào để xử lý (tổng số ngày: {total_days_to_process}).")
        actual_number_of_tasks = 0
    else:
        actual_number_of_tasks = NUMBER_OF_PARALLEL_TASKS

    if actual_number_of_tasks > 0:
        base_days_per_task = total_days_to_process // actual_number_of_tasks
        remaining_days = total_days_to_process % actual_number_of_tasks

        current_task_start_date = global_start_date
        for i in range(actual_number_of_tasks):
            days_for_this_task = base_days_per_task
            if remaining_days > 0:
                days_for_this_task += 1
                remaining_days -= 1
            
            # Đảm bảo task cuối cùng không lấy nhiều ngày hơn mức cần thiết
            if current_task_start_date + timedelta(days=days_for_this_task -1) > global_end_date:
                days_for_this_task = (global_end_date - current_task_start_date).days + 1

            if days_for_this_task <= 0: # Không nên xảy ra nếu logic trên đúng, nhưng để phòng ngừa
                continue

            current_task_end_date = current_task_start_date + timedelta(days=days_for_this_task - 1)

            DATE_INTERVALS_FOR_TASKS.append(
                (current_task_start_date.strftime("%Y%m%d"), current_task_end_date.strftime("%Y%m%d"))
            )
            logging.info(f"  Task {i+1}: {days_for_this_task} ngày - Từ {current_task_start_date.strftime('%Y%m%d')} đến {current_task_end_date.strftime('%Y%m%d')}")

            current_task_start_date = current_task_end_date + timedelta(days=1)
            if current_task_start_date > global_end_date:
                 break # Đã xử lý hết các ngày

    # Xử lý trường hợp total_days_to_process = 0 (không có ngày nào để xử lý)
    # Hoặc khi không có task nào được tạo dù có ngày (fallback rất hiếm)
    if not DATE_INTERVALS_FOR_TASKS and total_days_to_process > 0:
        logging.warning("Fallback: Không có khoảng ngày nào được tự động phân chia, tạo một task duy nhất cho toàn bộ khoảng thời gian.")
        DATE_INTERVALS_FOR_TASKS.append((PIPELINE_GLOBAL_START_DATE_STR, PIPELINE_GLOBAL_END_DATE_STR))
    elif not DATE_INTERVALS_FOR_TASKS and total_days_to_process <= 0:
        logging.info("Không có ngày nào để xử lý, sẽ không tạo task crawl nào.")


except ValueError as ve:
    logging.error(f"Lỗi khi xử lý ngày đầu vào: {ve}")
    DATE_INTERVALS_FOR_TASKS = []
except Exception as e:
    logging.error(f"Lỗi không xác định khi tính toán khoảng ngày: {e}", exc_info=True)
    DATE_INTERVALS_FOR_TASKS = []


logging.info(f"Các khoảng ngày cuối cùng được phân chia cho các task: {DATE_INTERVALS_FOR_TASKS}")
# --- END: Logic tính toán ---


def crawl_and_upload_callable(
    selenium_hub: str,
    start_date_to_process: str,
    end_date_to_process: str,
    s3_conn_id: str,
    s3_bucket_name: str,
    **kwargs
):
    log_prefix = f"[Interval: {start_date_to_process}-{end_date_to_process}]"
    logging.info(f"{log_prefix} Bắt đầu quá trình crawl trên Selenium Hub: {selenium_hub}")
    logging.info(f"{log_prefix} Sử dụng S3 connection ID: {s3_conn_id} cho bucket: {s3_bucket_name}")

    try:
        s3_hook_minio = S3Hook(aws_conn_id=s3_conn_id)
        logging.info(f"{log_prefix} Đã khởi tạo S3Hook thành công.")

        process_date_range_and_categories(
            selenium_hub_url=selenium_hub,
            start_date_str=start_date_to_process,
            end_date_str=end_date_to_process,
            s3_hook_for_minio=s3_hook_minio,
            minio_bucket_name=s3_bucket_name
        )
        logging.info(f"{log_prefix} Hàm process_date_range_and_categories đã hoàn thành.")

    except Exception as e:
        logging.error(f"{log_prefix} Lỗi trong quá trình crawl và upload: {e}", exc_info=True)
        raise

    logging.info(f"{log_prefix} Task crawl và upload hoàn tất thành công!")


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='DAG để crawl dữ liệu VnExpress song song, tự động chia ngày và tải lên MinIO',
    schedule=None,
    # Sử dụng pendulum để đặt start_date, tương thích với Airflow 2.2+ và 3.0+
    start_date=pendulum.now("Asia/Ho_Chi_Minh").subtract(days=1),
    # Hoặc một ngày cụ thể: pendulum.datetime(2023, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=['web_scraping', 'vnexpress', 'minio', 'dynamic_parallel_processing'],
) as dag:

    start_pipeline = EmptyOperator(
        task_id='start_pipeline_processing'
    )

    end_pipeline = EmptyOperator(
        task_id='end_pipeline_processing'
    )

    crawl_interval_tasks = []
    if not DATE_INTERVALS_FOR_TASKS:
        logging.warning("Không có khoảng ngày nào được phân chia để xử lý. Pipeline sẽ không tạo task crawl nào.")
    else:
        for idx, (start_date_str, end_date_str) in enumerate(DATE_INTERVALS_FOR_TASKS):
            task_id = f'crawl_vnexpress_part_{idx+1}_from_{start_date_str}_to_{end_date_str}'

            crawl_task = PythonOperator(
                task_id=task_id,
                python_callable=crawl_and_upload_callable,
                op_kwargs={
                    'selenium_hub': SELENIUM_HUB_URL,
                    'start_date_to_process': start_date_str,
                    'end_date_to_process': end_date_str,
                    's3_conn_id': MINIO_CONNECTION_ID,
                    's3_bucket_name': MINIO_BUCKET_NAME,
                }
            )
            crawl_interval_tasks.append(crawl_task)

    if crawl_interval_tasks:
        start_pipeline >> crawl_interval_tasks >> end_pipeline
    else: # Nếu không có task nào được tạo
        start_pipeline >> end_pipeline
        logging.info("Pipeline không có task crawl nào được tạo do không có khoảng ngày hợp lệ hoặc không có ngày nào để xử lý.")
