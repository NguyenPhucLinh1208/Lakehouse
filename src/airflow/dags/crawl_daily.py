from __future__ import annotations

import logging
import json
import pendulum

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from Web_scraping.SeleniumPackage import init_driver
from Web_scraping.CrawlJob import scrape_vnexpress_articles


DAG_ID = "vnexpress_category_crawl_single_day_dynamic_dag"
MINIO_CONNECTION_ID = "my_lakehouse_conn"
MINIO_BUCKET_NAME = "raw-news-lakehouse"
SELENIUM_HUB_URL = "http://selenium-hub:4444/wd/hub"
SELENIUM_POOL_NAME = "selenium_pool"

ALL_CATEGORY_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

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
### DAG Thu Thập Dữ Liệu VnExpress Theo Category (Một Ngày)

**Mục đích:**
- Thu thập dữ liệu bài báo từ VnExpress cho một ngày cụ thể (định dạng YYYYMMDD), xử lý từng *category_id* như một task riêng lẻ.
- Tự động điều phối tối đa số lượng luồng Selenium song song được cấu hình trong Airflow pool *{SELENIUM_POOL_NAME}* (khuyến nghị 3 luồng).
- Tải dữ liệu đã thu thập (dưới dạng JSON Lines cho từng topic) lên vùng raw MinIO S3.

**Luồng hoạt động:**
1.  ***generate_category_inputs***: Tạo danh sách các dictionary, mỗi dictionary chứa một *category_id* để truyền cho các task động.
2.  ***process_single_category*** (dynamic tasks): Với mỗi *category_id* được cung cấp:
    * Khởi tạo Selenium WebDriver (sử dụng pool *{SELENIUM_POOL_NAME}* để giới hạn số session đồng thời).
    * Gọi hàm *scrape_vnexpress_articles* với ngày ở định dạng YYYYMMDD để lấy link và nội dung chi tiết cho *category_id* đó.
    * Nếu có bài báo, lưu trữ kết quả dưới dạng file JSON Lines riêng cho *topic_name* tương ứng lên MinIO theo cấu trúc: *{{YYYY}}/{{MM}}/{{DD}}/{{topic_name_with_underscores}}.jsonl*.
    * Đảm bảo đóng WebDriver sau khi hoàn tất hoặc gặp lỗi.    
3.  ***summarize_uploads***: Tổng hợp và log kết quả của các task *process_single_category*.

**Ngày xử lý (Processing Date):**
- Được xác định bởi tham số *processing_date* (định dạng YYYYMMDD) khi trigger DAG thủ công.
- Mặc định là ngày hôm nay (theo giờ Asia/Ho_Chi_Minh, định dạng YYYYMMDD).

**Các Category IDs được xử lý:**
*{ALL_CATEGORY_IDS}*

**Yêu cầu cài đặt:**
- Tạo một Airflow Pool tên là *{SELENIUM_POOL_NAME}* với số lượng slot mong muốn (ví dụ: 3) trong Airflow UI (Admin -> Pools).
"""

@dag(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='Crawl VnExpress từng category (động), giới hạn bởi pool, lưu MinIO theo topic (JSONL).',
    schedule=None,
    catchup=False,
    tags=['web_scraping', 'vnexpress', 'minio', 'dynamic_tasks', 'selenium', 'jsonl'],
    doc_md=DAG_DOC_MD,
    params={
        "processing_date": Param(
            default=pendulum.now("Asia/Ho_Chi_Minh").format("YYYYMMDD"),
            type="string",
            title="Processing Date (YYYYMMDD)",
            description="Ngày (YYYYMMDD) cần thu thập dữ liệu. Mặc định là ngày hiện tại.",
        )
    }
)
def vnexpress_category_crawl_single_day_dynamic():
    task_logger = logging.getLogger("airflow.task")

    @task
    def generate_category_inputs(all_ids: list[int]) -> list[dict[str, int]]:
        if not all_ids:
            task_logger.warning("Danh sách ALL_CATEGORY_IDS trống. Sẽ không có task nào được tạo.")
            return []
        task_logger.info(f"Chuẩn bị inputs cho {len(all_ids)} categories: {all_ids}")
        return [{"category_id_to_process": cat_id} for cat_id in all_ids]

    @task(pool=SELENIUM_POOL_NAME)
    def process_single_category(
        category_processing_details: dict[str, int],
        processing_date_str: str,
        selenium_hub_url: str,
        s3_conn_id: str,
        s3_bucket_name: str
    ) -> str | None:
        category_id = category_processing_details["category_id_to_process"]
        task_logger.info(
            f"Bắt đầu xử lý category_id: {category_id} cho ngày (YYYYMMDD): {processing_date_str}."
        )

        driver = None
        uploaded_s3_key_path = None

        try:
            task_logger.info(f"Category {category_id}: Đang kết nối tới Selenium Hub: {selenium_hub_url}...")
            driver = init_driver(selenium_hub_url)
            if driver is None:
                task_logger.error(f"Category {category_id}: Không thể khởi tạo Selenium WebDriver. Hub: {selenium_hub_url}")
                raise ConnectionError(f"Failed to initialize WebDriver for category {category_id}")
            task_logger.info(f"Category {category_id}: Kết nối Selenium WebDriver thành công.")

            articles, determined_topic_name = scrape_vnexpress_articles(
                driver=driver,
                category_id=category_id,
                start_date_str=processing_date_str,
                end_date_str=processing_date_str
            )

            if not determined_topic_name:
                task_logger.warning(f"Category {category_id}: Không xác định được topic name. Bỏ qua category này.")
                return None

            if not articles:
                task_logger.info(
                    f"Category {category_id} (Topic: {determined_topic_name}): Không thu thập được bài báo nào."
                )
                return None

            task_logger.info(
                f"Category {category_id} (Topic: {determined_topic_name}): Thu thập được {len(articles)} bài báo."
            )
            
            for article_data in articles:
                if isinstance(article_data, dict):
                    article_data['source_category_id'] = category_id
                else:
                    task_logger.warning(f"Category {category_id}: Dữ liệu bài báo không phải là dict: {type(article_data)}")

            s3_hook = S3Hook(aws_conn_id=s3_conn_id)
            try:
                parsed_date = pendulum.parse(processing_date_str) 
                year = parsed_date.format("YYYY") 
                month = parsed_date.format("MM") 
                day = parsed_date.format("DD")   
            except ValueError:
                task_logger.error(
                    f"Category {category_id}: Định dạng processing_date_str (YYYYMMDD) không hợp lệ: {processing_date_str}. Không thể tạo S3 key."
                )
                raise

            s3_topic_name = determined_topic_name.replace(' ', '_').lower()
            s3_key = f"{year}/{month}/{day}/{s3_topic_name}.jsonl"

            json_lines_content = [json.dumps(article_dict, ensure_ascii=False, separators=(',', ':')) for article_dict in articles]
            jsonl_data_string = "\n".join(json_lines_content)
            jsonl_data_bytes_encoded = jsonl_data_string.encode('utf-8')

            task_logger.info(
                f"Category {category_id}: Đang tải {len(articles)} bài báo (JSONL) cho topic '{determined_topic_name}' "
                f"lên S3 key: s3://{s3_bucket_name}/{s3_key}, kích thước: {len(jsonl_data_bytes_encoded)} bytes."
            )

            s3_hook.load_bytes(
                bytes_data=jsonl_data_bytes_encoded,
                key=s3_key,
                bucket_name=s3_bucket_name,
                replace=True,
                acl_policy=None,
            )

            uploaded_s3_key_path = f"s3://{s3_bucket_name}/{s3_key}"
            task_logger.info(f"Category {category_id}: Đã tải thành công topic '{determined_topic_name}' lên {uploaded_s3_key_path}.")

        except ConnectionError as ce:
            task_logger.error(f"Category {category_id}: Lỗi kết nối WebDriver: {ce}", exc_info=True)
        except Exception as e:
            task_logger.error(
                f"Category {category_id}: Lỗi nghiêm trọng trong quá trình xử lý: {e}",
                exc_info=True
            )
        finally:
            if driver:
                task_logger.info(f"Category {category_id}: Đang đóng Selenium WebDriver...")
                try:
                    driver.quit()
                    task_logger.info(f"Category {category_id}: Đã đóng Selenium WebDriver.")
                except Exception as e_quit:
                    task_logger.error(f"Category {category_id}: Lỗi khi đóng WebDriver: {e_quit}", exc_info=True)
        
        return uploaded_s3_key_path

    @task
    def summarize_uploads(uploaded_keys_from_all_tasks: list[str | None]):
        successful_uploads = [key for key in uploaded_keys_from_all_tasks if key is not None]
        tasks_without_uploads_count = len([res for res in uploaded_keys_from_all_tasks if res is None])

        task_logger.info("----- TỔNG KẾT QUÁ TRÌNH THU THẬP DỮ LIỆU -----")
        if successful_uploads:
            task_logger.info(f"Tổng số file đã được tải lên thành công: {len(successful_uploads)}")
            for s3_path in successful_uploads:
                task_logger.info(f"  - {s3_path}")
        else:
            task_logger.info("Không có file nào được tải lên S3 thành công trong lần chạy này.")

        if tasks_without_uploads_count > 0:
            task_logger.warning(
                f"Tổng số task xử lý category không tải lên file (do lỗi, không có dữ liệu, hoặc không tìm thấy topic): {tasks_without_uploads_count}"
            )
        else:
            task_logger.info("Tất cả các task xử lý category đều có kết quả (nếu có dữ liệu để tải lên).")
        
        total_tasks_processed = len(uploaded_keys_from_all_tasks)
        task_logger.info(f"Tổng số category được đưa vào xử lý: {total_tasks_processed}")
        
        return {
            "total_categories_processed": total_tasks_processed,
            "successful_upload_count": len(successful_uploads),
            "tasks_without_uploads": tasks_without_uploads_count,
            "list_successful_files": successful_uploads
        }

    start_pipeline = EmptyOperator(task_id='Bắt_đầu_pipeline')
    end_pipeline = EmptyOperator(task_id='Kết_thúc_pipeline')

    processing_date_param_value = "{{ params.processing_date }}"
    
    category_inputs_list_task_output = generate_category_inputs(all_ids=ALL_CATEGORY_IDS)

    crawl_task_results = process_single_category.partial(
        processing_date_str=processing_date_param_value,
        selenium_hub_url=SELENIUM_HUB_URL,
        s3_conn_id=MINIO_CONNECTION_ID,
        s3_bucket_name=MINIO_BUCKET_NAME,
    ).expand(category_processing_details=category_inputs_list_task_output)

    summary_reporting_task = summarize_uploads(uploaded_keys_from_all_tasks=crawl_task_results)

    start_pipeline >> category_inputs_list_task_output >> crawl_task_results >> summary_reporting_task >> end_pipeline

vnexpress_dag_dynamic_instance = vnexpress_category_crawl_single_day_dynamic()
