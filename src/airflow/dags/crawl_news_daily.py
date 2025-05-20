from __future__ import annotations

import logging
import json
from datetime import datetime

import pendulum

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from Web_scraping.SeleniumPackage import init_driver
from Web_scraping.CrawlJob import scrape_vnexpress_articles

DAG_ID = "vnexpress_category_crawl_single_day_dag"
MINIO_CONNECTION_ID = "my_lakehouse_conn"
MINIO_BUCKET_NAME = "raw-news-lakehouse"
SELENIUM_HUB_URL = "http://selenium-hub:4444/wd/hub"

CATEGORY_GROUPS = [
    [1, 2, 3],
    [4, 5, 6, 7],
    [8, 9, 10, 11]
]

DEFAULT_ARGS = {
    'owner': 'airflow_user',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=2),
    'email_on_failure': False,
    'email_on_retry': False,
}

DAG_DOC_MD = f"""
### DAG Thu Thập Dữ Liệu VnExpress Theo Category (Một Ngày)

**Mục đích:**
- Thu thập dữ liệu bài báo từ VnExpress cho một ngày cụ thể (định dạng YYYYMMDD), được chia theo các nhóm category.
- Luôn chạy 3 luồng song song, mỗi luồng xử lý một nhóm category.
- Tải dữ liệu đã thu thập (dưới dạng JSON Lines cho từng topic) lên MinIO S3.

**Luồng hoạt động:**
1.  **`process_category_group` (3 tasks song song)**: Với mỗi nhóm category:
    * Khởi tạo Selenium WebDriver.
    * Lặp qua từng `category_id` trong nhóm:
        * Gọi hàm `scrape_vnexpress_articles` với ngày ở định dạng YYYYMMDD để lấy link và nội dung chi tiết.
    * Tổng hợp và nhóm các bài báo thu được theo `topic_name`.
    * Với mỗi `topic_name`, lưu trữ kết quả dưới dạng file JSON Lines riêng lên MinIO theo cấu trúc: `{{YYYY}}/{{MM}}/{{DD}}/{{topic_name_with_underscores}}.jsonl`.
    * Đảm bảo đóng WebDriver sau khi hoàn tất hoặc gặp lỗi.

**Ngày xử lý (Processing Date):**
- Được xác định bởi tham số `processing_date` (định dạng YYYYMMDD) khi trigger DAG thủ công.
- Mặc định là ngày hôm nay (theo giờ Asia/Ho_Chi_Minh, định dạng YYYYMMDD).

**Nhóm Category được xử lý song song:**
- Nhóm 1: {CATEGORY_GROUPS[0]}
- Nhóm 2: {CATEGORY_GROUPS[1]}
- Nhóm 3: {CATEGORY_GROUPS[2]}
"""

@dag(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='Crawl VnExpress theo category cho một ngày (YYYYMMDD), 3 luồng song song, lưu MinIO theo topic (JSONL).',
    schedule=None,
    catchup=False,
    tags=['web_scraping', 'vnexpress', 'minio', 'category_parallel', 'selenium', 'jsonl_output'],
    doc_md=DAG_DOC_MD,
    params={
        "processing_date": Param(
            default=pendulum.now("Asia/Ho_Chi_Minh").format("YYYYMMDD"), # Thay đổi định dạng mặc định
            type="string",
            title="Processing Date (YYYYMMDD)", # Cập nhật title
            description="Ngày (YYYYMMDD) cần thu thập dữ liệu. Mặc định là ngày hiện tại.", # Cập nhật mô tả
        )
    }
)
def vnexpress_category_crawl_single_day():
    """
    DAG chính điều phối việc thu thập dữ liệu theo nhóm category song song,
    lưu kết quả theo từng topic (JSONL) vào S3. Ngày xử lý ở định dạng YYYYMMDD.
    """

    @task
    def process_category_group(
        category_group: list[int],
        group_index: int,
        processing_date_str: str, # Sẽ nhận giá trị YYYYMMDD
        selenium_hub_url: str,
        s3_conn_id: str,
        s3_bucket_name: str
    ):
        """
        Xử lý một nhóm các category: khởi tạo driver, scrape (với ngày YYYYMMDD),
        nhóm theo topic, định dạng JSON Lines và upload lên S3.
        Trả về danh sách các S3 keys đã được tải lên.
        """
        task_logger = logging.getLogger("airflow.task")
        task_logger.info(
            f"Bắt đầu xử lý nhóm category {group_index + 1}: {category_group} "
            f"cho ngày (YYYYMMDD): {processing_date_str}"
        )

        driver = None
        articles_by_topic_in_group: dict[str, list] = {}
        uploaded_s3_keys = []

        try:
            task_logger.info(f"Đang kết nối tới Selenium Hub: {selenium_hub_url}")
            driver = init_driver(selenium_hub_url)
            task_logger.info(f"Đã kết nối Selenium WebDriver cho nhóm {group_index + 1}.")

            for category_id in category_group:
                task_logger.info(f"Nhóm {group_index + 1}: Bắt đầu scrape cho category_id: {category_id}")
                # scrape_vnexpress_articles sẽ nhận processing_date_str ở định dạng YYYYMMDD
                articles, determined_topic_name = scrape_vnexpress_articles(
                    driver=driver,
                    category_id=category_id,
                    start_date_str=processing_date_str,
                    end_date_str=processing_date_str
                )

                if not determined_topic_name:
                    task_logger.warning(f"Nhóm {group_index + 1}, Category {category_id}: Không xác định được topic name.")
                    continue

                if articles:
                    task_logger.info(
                        f"Nhóm {group_index + 1}, Category {category_id} (Topic: {determined_topic_name}): Thu thập được {len(articles)} bài báo."
                    )
                    if determined_topic_name not in articles_by_topic_in_group:
                        articles_by_topic_in_group[determined_topic_name] = []
                    
                    for article in articles:
                        article['source_category_id'] = category_id
                    articles_by_topic_in_group[determined_topic_name].extend(articles)
                else:
                    task_logger.info(
                        f"Nhóm {group_index + 1}, Category {category_id} (Topic: {determined_topic_name}): Không thu thập được bài báo nào."
                    )
        except Exception as e:
            task_logger.error(
                f"Lỗi nghiêm trọng trong quá trình xử lý nhóm category {group_index + 1} ({category_group}): {e}",
                exc_info=True
            )
            raise
        finally:
            if driver:
                task_logger.info(f"Đang đóng Selenium WebDriver cho nhóm {group_index + 1}.")
                driver.quit()
            task_logger.info(f"Đã đóng Selenium WebDriver cho nhóm {group_index + 1}.")

        if not articles_by_topic_in_group:
            task_logger.warning(f"Nhóm {group_index + 1}: Không có bài báo nào được thu thập từ bất kỳ category nào. Bỏ qua việc upload.")
            return []

        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        try:
            # Parse processing_date_str (YYYYMMDD) để lấy year, month, day cho S3 key
            parsed_date = datetime.strptime(processing_date_str, "%Y%m%d") # Sửa định dạng parse
            year = parsed_date.strftime("%Y")
            month = parsed_date.strftime("%m")
            day = parsed_date.strftime("%d")
        except ValueError:
            task_logger.error(f"Định dạng processing_date_str (YYYYMMDD) không hợp lệ: {processing_date_str}. Không thể tạo S3 key.")
            raise ValueError(f"Invalid processing_date_str format (expected YYYYMMDD): {processing_date_str}")


        for topic_name, topic_articles_list in articles_by_topic_in_group.items():
            if not topic_articles_list:
                task_logger.info(f"Nhóm {group_index + 1}: Topic '{topic_name}' không có bài báo nào, bỏ qua upload.")
                continue
            
            try:
                s3_topic_name = topic_name.replace(' ', '_')
                s3_key = f"{year}/{month}/{day}/{s3_topic_name}.jsonl"
                
                json_lines = [json.dumps(article_dict, ensure_ascii=False, separators=(',', ':')) for article_dict in topic_articles_list]
                jsonl_data_str = "\n".join(json_lines)
                jsonl_data_bytes = jsonl_data_str.encode('utf-8')
                
                task_logger.info(
                    f"Nhóm {group_index + 1}: Đang tải {len(topic_articles_list)} bài báo (JSONL) cho topic '{topic_name}' "
                    f"(S3 Key: {s3_key}), kích thước: {len(jsonl_data_bytes)} bytes."
                )
                
                s3_client = s3_hook.get_conn()
                s3_client.put_object(
                    Bucket=s3_bucket_name,
                    Key=s3_key,
                    Body=jsonl_data_bytes,
                    ContentType='application/x-jsonlines'
                )
                
                task_logger.info(f"Nhóm {group_index + 1}: Đã tải thành công cho topic '{topic_name}'.")
                uploaded_s3_keys.append(f"s3://{s3_bucket_name}/{s3_key}")
            except Exception as e:
                task_logger.error(
                    f"Nhóm {group_index + 1}: Lỗi khi tải dữ liệu cho topic '{topic_name}' (S3 Key: {s3_key}) lên S3: {e}",
                    exc_info=True
                )

        if not uploaded_s3_keys:
            task_logger.warning(f"Nhóm {group_index + 1}: Không tải lên được file S3 nào.")
        return uploaded_s3_keys


    start_pipeline = EmptyOperator(task_id='start_pipeline_processing')
    end_pipeline = EmptyOperator(task_id='end_pipeline_processing')

    processing_date_param = "{{ params.processing_date }}" # Sẽ nhận giá trị YYYYMMDD từ params
    
    expanded_params = []
    for i, group in enumerate(CATEGORY_GROUPS):
        expanded_params.append({"category_group": group, "group_index": i})

    crawl_tasks = process_category_group.partial(
        processing_date_str=processing_date_param, # Truyền ngày định dạng YYYYMMDD
        selenium_hub_url=SELENIUM_HUB_URL,
        s3_conn_id=MINIO_CONNECTION_ID,
        s3_bucket_name=MINIO_BUCKET_NAME,
    ).expand_kwargs(expanded_params)

    start_pipeline >> crawl_tasks >> end_pipeline

vnexpress_category_crawl_dag_instance = vnexpress_category_crawl_single_day()
