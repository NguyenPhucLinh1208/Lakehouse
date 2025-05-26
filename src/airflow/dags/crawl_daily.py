from __future__ import annotations

import logging
import json
from datetime import datetime

import pendulum

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Đảm bảo các module này có trong PYTHONPATH hoặc thư mục plugins của Airflow
# Ví dụ: bạn có thể đặt thư mục Web_scraping vào thư mục dags hoặc plugins
try:
    from Web_scraping.SeleniumPackage import init_driver
    from Web_scraping.CrawlJob import scrape_vnexpress_articles
except ImportError:
    # Fallback nếu không tìm thấy, hữu ích cho việc parse DAG ban đầu trên Airflow UI
    # nhưng sẽ gây lỗi khi task thực thi nếu module không thực sự tồn tại.
    logging.warning("Không thể import Web_scraping.SeleniumPackage hoặc Web_scraping.CrawlJob. "
                    "Đảm bảo chúng có trong PYTHONPATH hoặc thư mục plugins.")
    # Định nghĩa hàm giả để Airflow có thể parse DAG
    def init_driver(selenium_hub_url): return None
    def scrape_vnexpress_articles(driver, category_id, start_date_str, end_date_str): return [], None


DAG_ID = "vnexpress_category_crawl_single_day_dynamic_dag" # Đổi tên DAG ID nếu cần
MINIO_CONNECTION_ID = "my_lakehouse_conn"
MINIO_BUCKET_NAME = "raw-news-lakehouse"
SELENIUM_HUB_URL = "http://selenium-hub:4444/wd/hub"
# Tên của Airflow Pool bạn sẽ tạo trong UI (ví dụ: selenium_pool với 3 slots)
SELENIUM_POOL_NAME = "selenium_pool"

# Danh sách tất cả các category_id sẽ được xử lý
ALL_CATEGORY_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

DEFAULT_ARGS = {
    'owner': 'airflow_user',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    'retries': 1, # Có thể tăng retries cho các task scrape nếu cần
    'retry_delay': pendulum.duration(minutes=3), # Tăng thời gian chờ retry
    'email_on_failure': False,
    'email_on_retry': False,
}

DAG_DOC_MD = f"""
### DAG Thu Thập Dữ Liệu VnExpress Theo Category (Một Ngày) - Dynamic Tasks V2

**Mục đích:**
- Thu thập dữ liệu bài báo từ VnExpress cho một ngày cụ thể (định dạng YYYYMMDD), xử lý từng `category_id` như một task riêng lẻ.
- Tự động điều phối tối đa số lượng luồng Selenium song song được cấu hình trong Airflow pool `{SELENIUM_POOL_NAME}` (khuyến nghị 3 luồng).
- Tải dữ liệu đã thu thập (dưới dạng JSON Lines cho từng topic) lên MinIO S3.

**Luồng hoạt động:**
1.  **`generate_category_inputs`**: Tạo danh sách các dictionary, mỗi dictionary chứa một `category_id` để truyền cho các task động.
2.  **`process_single_category` (dynamic tasks)**: Với mỗi `category_id` được cung cấp:
    * Khởi tạo Selenium WebDriver (sử dụng pool `{SELENIUM_POOL_NAME}` để giới hạn số session đồng thời).
    * Gọi hàm `scrape_vnexpress_articles` với ngày ở định dạng YYYYMMDD để lấy link và nội dung chi tiết cho `category_id` đó.
    * Nếu có bài báo, lưu trữ kết quả dưới dạng file JSON Lines riêng cho `topic_name` tương ứng lên MinIO theo cấu trúc: `{{YYYY}}/{{MM}}/{{DD}}/{{topic_name_with_underscores}}.jsonl`.
    * Đảm bảo đóng WebDriver sau khi hoàn tất hoặc gặp lỗi.
    * **Lưu ý quan trọng**: Nếu nhiều `category_id` khác nhau sau khi scrape lại cho ra cùng một `determined_topic_name`, file S3 tương ứng với `topic_name` đó có thể bị ghi đè bởi task hoàn thành sau cùng cho topic đó. Nếu yêu cầu là phải gộp tất cả bài viết từ các `category_id` khác nhau vào một file duy nhất cho `topic_name` đó, cần phải có một bước tổng hợp dữ liệu sau khi tất cả các task `process_single_category` hoàn thành. Hiện tại, DAG này không thực hiện bước tổng hợp đó.
3.  **`summarize_uploads`**: Tổng hợp và log kết quả của các task `process_single_category`.

**Ngày xử lý (Processing Date):**
- Được xác định bởi tham số `processing_date` (định dạng YYYYMMDD) khi trigger DAG thủ công.
- Mặc định là ngày hôm nay (theo giờ Asia/Ho_Chi_Minh, định dạng YYYYMMDD).

**Các Category IDs được xử lý:**
`{ALL_CATEGORY_IDS}`

**Yêu cầu cài đặt:**
- Tạo một Airflow Pool tên là `{SELENIUM_POOL_NAME}` với số lượng slot mong muốn (ví dụ: 3) trong Airflow UI (Admin -> Pools).
- Đảm bảo các thư viện `Web_scraping.SeleniumPackage` và `Web_scraping.CrawlJob` có thể được import bởi Airflow worker.
"""

@dag(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='Crawl VnExpress từng category (động), giới hạn bởi pool, lưu MinIO theo topic (JSONL).',
    schedule=None, # Không có lịch chạy tự động, chỉ trigger thủ công hoặc qua API
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
def vnexpress_category_crawl_single_day_dynamic_v2():
    """
    DAG điều phối việc thu thập dữ liệu VnExpress cho từng category một cách động.
    Mỗi category_id sẽ được xử lý bởi một task riêng.
    Sử dụng Airflow Pool để giới hạn số lượng Selenium session chạy đồng thời.
    Kết quả được lưu dưới dạng JSON Lines cho mỗi topic vào S3 (MinIO).
    """
    task_logger = logging.getLogger("airflow.task") # Sử dụng logger chuẩn của Airflow task

    @task
    def generate_category_inputs(all_ids: list[int]) -> list[dict[str, int]]:
        """
        Chuẩn bị danh sách các dictionary đầu vào cho các task động.
        Mỗi dictionary sẽ chứa một 'category_id_to_process'.
        """
        if not all_ids:
            task_logger.warning("Danh sách ALL_CATEGORY_IDS trống. Sẽ không có task nào được tạo.")
            return []
        task_logger.info(f"Chuẩn bị inputs cho {len(all_ids)} categories: {all_ids}")
        return [{"category_id_to_process": cat_id} for cat_id in all_ids]

    @task(pool=SELENIUM_POOL_NAME) # Gán task này vào Airflow Pool đã định nghĩa
    def process_single_category(
        category_processing_details: dict[str, int], # Nhận dict ví dụ: {'category_id_to_process': 1}
        processing_date_str: str, # Ngày xử lý định dạng YYYYMMDD
        selenium_hub_url: str,
        s3_conn_id: str,
        s3_bucket_name: str
    ) -> str | None: # Trả về S3 key (str) nếu thành công, None nếu thất bại hoặc không có dữ liệu
        """
        Xử lý việc scrape dữ liệu cho một category_id duy nhất.
        Khởi tạo WebDriver, scrape, xử lý dữ liệu, và tải lên S3.
        """
        category_id = category_processing_details["category_id_to_process"]
        task_logger.info(
            f"Bắt đầu xử lý category_id: {category_id} cho ngày (YYYYMMDD): {processing_date_str}."
        )

        driver = None
        uploaded_s3_key_path = None

        try:
            task_logger.info(f"Category {category_id}: Đang kết nối tới Selenium Hub: {selenium_hub_url}...")
            driver = init_driver(selenium_hub_url) # Khởi tạo WebDriver
            if driver is None: # Kiểm tra nếu init_driver thất bại (ví dụ do fallback)
                task_logger.error(f"Category {category_id}: Không thể khởi tạo Selenium WebDriver. Hub: {selenium_hub_url}")
                raise ConnectionError(f"Failed to initialize WebDriver for category {category_id}")
            task_logger.info(f"Category {category_id}: Kết nối Selenium WebDriver thành công.")

            # Gọi hàm scrape_vnexpress_articles
            articles, determined_topic_name = scrape_vnexpress_articles(
                driver=driver,
                category_id=category_id,
                start_date_str=processing_date_str, # Ngày bắt đầu (YYYYMMDD)
                end_date_str=processing_date_str    # Ngày kết thúc (YYYYMMDD) - giống nhau cho 1 ngày
            )

            if not determined_topic_name:
                task_logger.warning(f"Category {category_id}: Không xác định được topic name. Bỏ qua category này.")
                return None # Trả về None để báo hiệu không có file nào được upload

            if not articles:
                task_logger.info(
                    f"Category {category_id} (Topic: {determined_topic_name}): Không thu thập được bài báo nào."
                )
                return None

            task_logger.info(
                f"Category {category_id} (Topic: {determined_topic_name}): Thu thập được {len(articles)} bài báo."
            )
            
            # Gán source_category_id cho từng bài báo
            for article_data in articles:
                if isinstance(article_data, dict): # Đảm bảo article_data là dict
                    article_data['source_category_id'] = category_id
                else:
                    task_logger.warning(f"Category {category_id}: Dữ liệu bài báo không phải là dict: {type(article_data)}")


            # Xử lý và tải lên S3
            s3_hook = S3Hook(aws_conn_id=s3_conn_id)
            try:
                # Parse ngày YYYYMMDD để lấy year, month, day cho cấu trúc thư mục S3
                parsed_date = datetime.strptime(processing_date_str, "%Y%m%d")
                year = parsed_date.strftime("%Y")
                month = parsed_date.strftime("%m")
                day = parsed_date.strftime("%d")
            except ValueError:
                task_logger.error(
                    f"Category {category_id}: Định dạng processing_date_str (YYYYMMDD) không hợp lệ: {processing_date_str}. Không thể tạo S3 key."
                )
                raise # Re-raise lỗi để task này được đánh dấu là thất bại

            # Tạo S3 key từ topic_name (thay thế khoảng trắng bằng '_')
            s3_topic_name = determined_topic_name.replace(' ', '_').lower() # Chuẩn hóa tên topic
            s3_key = f"{year}/{month}/{day}/{s3_topic_name}.jsonl"

            # Chuyển đổi danh sách các dictionary bài báo thành chuỗi JSON Lines
            json_lines_content = [json.dumps(article_dict, ensure_ascii=False, separators=(',', ':')) for article_dict in articles]
            jsonl_data_string = "\n".join(json_lines_content)
            jsonl_data_bytes_encoded = jsonl_data_string.encode('utf-8')

            task_logger.info(
                f"Category {category_id}: Đang tải {len(articles)} bài báo (JSONL) cho topic '{determined_topic_name}' "
                f"lên S3 key: s3://{s3_bucket_name}/{s3_key}, kích thước: {len(jsonl_data_bytes_encoded)} bytes."
            )

            # Sử dụng S3Hook để tải file lên (thay vì get_conn() rồi dùng put_object của boto3 client)
            # phương thức load_bytes đơn giản hơn cho trường hợp này
            s3_hook.load_bytes(
                bytes_data=jsonl_data_bytes_encoded,
                key=s3_key,
                bucket_name=s3_bucket_name,
                replace=True, # Ghi đè nếu file đã tồn tại
                acl_policy=None, # Hoặc 'public-read' nếu bạn muốn công khai
                # content_type='application/x-jsonlines' # S3Hook.load_bytes không có param này, nó tự xác định hoặc dùng default
            )
            # Để set ContentType, bạn có thể cần dùng get_conn() và put_object như code gốc
            # s3_client = s3_hook.get_conn()
            # s3_client.put_object(
            #     Bucket=s3_bucket_name,
            #     Key=s3_key,
            #     Body=jsonl_data_bytes_encoded,
            #     ContentType='application/x-jsonlines'
            # )

            uploaded_s3_key_path = f"s3://{s3_bucket_name}/{s3_key}"
            task_logger.info(f"Category {category_id}: Đã tải thành công topic '{determined_topic_name}' lên {uploaded_s3_key_path}.")

        except ConnectionError as ce: # Bắt lỗi kết nối WebDriver cụ thể
             task_logger.error(f"Category {category_id}: Lỗi kết nối WebDriver: {ce}", exc_info=True)
             # Không re-raise để cho các category khác tiếp tục, task này sẽ fail
        except Exception as e:
            task_logger.error(
                f"Category {category_id}: Lỗi nghiêm trọng trong quá trình xử lý: {e}",
                exc_info=True # Log kèm theo traceback
            )
            # Không re-raise ở đây để cho phép các category khác tiếp tục xử lý.
            # Airflow sẽ tự động đánh dấu task instance này là thất bại.
        finally:
            if driver:
                task_logger.info(f"Category {category_id}: Đang đóng Selenium WebDriver...")
                try:
                    driver.quit()
                    task_logger.info(f"Category {category_id}: Đã đóng Selenium WebDriver.")
                except Exception as e_quit:
                    task_logger.error(f"Category {category_id}: Lỗi khi đóng WebDriver: {e_quit}", exc_info=True)
        
        return uploaded_s3_key_path # Trả về S3 key path hoặc None

    @task
    def summarize_uploads(uploaded_keys_from_all_tasks: list[str | None]):
        """
        Tổng hợp và log kết quả từ tất cả các task `process_single_category`.
        `uploaded_keys_from_all_tasks` là một list các giá trị trả về (S3 key path hoặc None) từ mỗi task động.
        """
        successful_uploads = [key for key in uploaded_keys_from_all_tasks if key is not None]
        # Đếm số task không trả về key (có thể do lỗi, hoặc không có dữ liệu, hoặc không xác định được topic)
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

    # Định nghĩa luồng công việc của DAG
    start_pipeline = EmptyOperator(task_id='Bắt_đầu_pipeline')
    end_pipeline = EmptyOperator(task_id='Kết_thúc_pipeline')

    # Lấy giá trị tham số ngày xử lý từ params của DAG
    processing_date_param_value = "{{ params.processing_date }}"
    
    # Bước 1: Tạo danh sách các đầu vào cho task động
    # Output của task này (một list các dict) sẽ được dùng cho .expand()
    category_inputs_list_task_output = generate_category_inputs(all_ids=ALL_CATEGORY_IDS)

    # Bước 2: Áp dụng dynamic task mapping cho `process_single_category`
    # Mỗi item trong `category_inputs_list_task_output` sẽ tạo ra một task `process_single_category` riêng.
    # Airflow Pool sẽ giới hạn số lượng các task này chạy đồng thời.
    crawl_task_results = process_single_category.partial(
        processing_date_str=processing_date_param_value, # Truyền ngày định dạng YYYYMMDD
        selenium_hub_url=SELENIUM_HUB_URL,
        s3_conn_id=MINIO_CONNECTION_ID,
        s3_bucket_name=MINIO_BUCKET_NAME,
    ).expand(category_processing_details=category_inputs_list_task_output) # Ánh xạ động qua danh sách đầu vào

    # Bước 3: Task tổng hợp kết quả sau khi tất cả các task crawl hoàn thành
    summary_reporting_task = summarize_uploads(uploaded_keys_from_all_tasks=crawl_task_results)

    # Thiết lập thứ tự chạy của các task
    start_pipeline >> category_inputs_list_task_output >> crawl_task_results >> summary_reporting_task >> end_pipeline

# Khởi tạo instance của DAG
vnexpress_dag_dynamic_instance = vnexpress_category_crawl_single_day_dynamic_v2()
