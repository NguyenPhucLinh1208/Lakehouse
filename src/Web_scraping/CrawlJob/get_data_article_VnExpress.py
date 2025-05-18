from Web_scraping.SeleniumPackage import (
    init_driver,
    click_element
)
from Web_scraping.CrawlPackage import (
    create_url_vnexpress,
    next_page_status,
    get_links_VnExpress,
    get_content_article
)
import json
import logging
import sys
import io
import datetime

# Import S3Hook for Airflow connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # Giả sử đã cài đặt

logger = logging.getLogger(__name__)

CATEGORY_ID_TO_TOPIC_NAME = {
    1: "Thời sự",
    2: "Góc nhìn",
    3: "Thế giới",
    4: "Kinh doanh",
    5: "Bất động sản",
    6: "Pháp luật",
    7: "Giáo dục",
    8: "Sức khỏe",
    9: "Đời sống",
    10: "Du lịch",
    11: "Khoa học"
}

def scrape_vnexpress_articles(driver, category_id: int, start_date_str: str, end_date_str: str):
    determined_topic_name = CATEGORY_ID_TO_TOPIC_NAME.get(category_id)

    if not determined_topic_name:
        logger.error(f"Category ID không hợp lệ hoặc chưa được hỗ trợ: {category_id}. Vui lòng kiểm tra lại bản đồ CATEGORY_ID_TO_TOPIC_NAME.")
        return [], None

    logger.info(f"Bắt đầu hàm scrape_vnexpress_articles với category_id: {category_id} (Topic: {determined_topic_name}), start_date: {start_date_str}, end_date: {end_date_str}")

    try:
        url = create_url_vnexpress(category_id, start_date_str, end_date_str)
        if not url:
            logger.error("Không thể tạo URL. Vui lòng kiểm tra lại các tham số đầu vào.")
            return [], determined_topic_name
        logger.info(f"Đã tạo URL thành công: {url}")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình tạo URL (create_url_vnexpress): {e}")
        return [], determined_topic_name

    collected_links = []
    next_page_xpath = "//a[contains(@class, 'btn-page') and contains(@class, 'next-page')]"

    try:
        driver.get(url)
        logger.info(f"Đã truy cập URL: {url}")
        page_count = 1
        while True:
            logger.info(f"Đang xử lý trang số: {page_count}...")
            current_page_has_next = next_page_status(driver)

            html = driver.page_source
            links_in_page = get_links_VnExpress(html, determined_topic_name)

            if links_in_page:
                logger.info(f"Tìm thấy {len(links_in_page)} link(s) thuộc chủ đề '{determined_topic_name}' trên trang {page_count}.")
                collected_links.extend(links_in_page)
            else:
                logger.info(f"Không tìm thấy link nào thuộc chủ đề '{determined_topic_name}' trên trang {page_count}.")

            if current_page_has_next:
                logger.info("Phát hiện có trang tiếp theo. Đang nhấp vào nút 'next-page'...")
                click_element(driver, next_page_xpath)
                page_count += 1
            else:
                logger.info("Không còn trang tiếp theo hoặc không tìm thấy nút 'next-page'. Kết thúc việc thu thập links.")
                break
    except Exception as e:
        logger.error(f"Lỗi xảy ra trong quá trình duyệt trang và thu thập links: {e}")

    if not collected_links:
        logger.info("Không thu thập được link bài báo nào. Kết thúc hàm.")
        return [], determined_topic_name

    logger.info(f"Tổng cộng đã thu thập được {len(collected_links)} link(s). Bắt đầu lấy nội dung chi tiết...")

    article_contents = []
    for index, link_info in enumerate(collected_links):
        url_link = link_info.get('url')
        if not url_link:
            logger.warning(f"Link #{index + 1} không có key 'url'. Bỏ qua link này.")
            continue

        logger.info(f"Đang lấy nội dung cho link #{index + 1}: {url_link}")
        try:
            content = {}
            content["title"] = link_info.get('title')
            content["url"] = url_link
            content["description"] = link_info.get('description')
            content["topic"] = link_info.get('topic')
            content["sub_topic"] = link_info.get('sub_topic')
            content.update(get_content_article(driver, url_link))
            article_contents.append(content)
        except Exception as e:
            logger.error(f"Lỗi khi lấy nội dung cho bài báo tại URL {url_link}: {e}")

    logger.info(f"Hoàn tất việc lấy nội dung cho {len(article_contents)} bài báo.")
    return article_contents, determined_topic_name

def upload_data_to_minio(
    s3_hook_instance: S3Hook, # Nhận vào đối tượng S3Hook đã được khởi tạo
    bucket_name: str,
    object_name: str,
    data_bytes: bytes,
    content_type: str = 'application/octet-stream'
):
    """
    Tải dữ liệu lên MinIO/S3 sử dụng đối tượng S3Hook được truyền vào.
    """
    if not s3_hook_instance:
        logger.error("Đối tượng S3Hook chưa được cung cấp. Không thể tải lên.")
        return False
    try:
        s3_hook_instance.load_bytes(
            bytes_data=data_bytes,
            key=object_name,
            bucket_name=bucket_name,
            replace=True,
            # extra_args={'ContentType': content_type} # Cân nhắc nếu cần thiết
        )
        logger.info(f"Đã tải thành công '{object_name}' lên bucket '{bucket_name}'.")
        # endpoint_url = s3_hook_instance.get_conn().meta.endpoint_url if hasattr(s3_hook_instance.get_conn(), 'meta') else "your-minio-endpoint"
        # logger.info(f"Đối tượng có sẵn tại (ước tính): {endpoint_url}/{bucket_name}/{object_name}")
        return True
    except Exception as e:
        logger.error(f"Lỗi khi tải lên MinIO/S3 (object: {object_name}) bằng S3Hook: {e}", exc_info=True)
        return False

def process_date_range_and_categories(
    selenium_hub_url: str,
    start_date_str: str,
    end_date_str: str,
    s3_hook_for_minio: S3Hook, # Thay vì minio_conn_id, giờ đây là đối tượng hook
    minio_bucket_name: str
):
    logger.info(f"Bắt đầu quá trình xử lý cho khoảng ngày từ {start_date_str} đến {end_date_str}.")

    if not s3_hook_for_minio:
        logger.error("Đối tượng S3Hook cho MinIO chưa được cung cấp. Script sẽ dừng.")
        return

    driver = None
    try:
        driver = init_driver(selenium_hub_url)
        if not driver:
            logger.error("Không thể khởi tạo Selenium WebDriver. Script kết thúc.")
            return

        start_date = datetime.datetime.strptime(start_date_str, "%Y%m%d")
        end_date = datetime.datetime.strptime(end_date_str, "%Y%m%d")
        current_date = start_date

        while current_date <= end_date:
            crawl_date_str_format = current_date.strftime("%Y%m%d")
            year = current_date.strftime("%Y")
            month = current_date.strftime("%m")
            day = current_date.strftime("%d")

            logger.info(f"======== Đang xử lý cho ngày: {crawl_date_str_format} ========")

            for category_id in CATEGORY_ID_TO_TOPIC_NAME.keys():
                topic_name_from_map = CATEGORY_ID_TO_TOPIC_NAME.get(category_id)
                logger.info(f"--- Bắt đầu thu thập cho Category ID: {category_id} (Topic: {topic_name_from_map}) ---")

                try:
                    if not driver:
                        logger.error("WebDriver không hợp lệ. Bỏ qua category này.")
                        continue

                    all_articles_content, determined_topic_name_from_scrape = scrape_vnexpress_articles(
                        driver=driver,
                        category_id=category_id,
                        start_date_str=crawl_date_str_format,
                        end_date_str=crawl_date_str_format
                    )

                    actual_topic_name = determined_topic_name_from_scrape if determined_topic_name_from_scrape else topic_name_from_map

                    if not actual_topic_name:
                        logger.warning(f"Không thể xác định tên topic cho category_id {category_id}. Bỏ qua.")
                        continue

                    if all_articles_content:
                        logger.info(f"Đã thu thập {len(all_articles_content)} bài báo cho topic '{actual_topic_name}' ngày {crawl_date_str_format}.")
                        json_lines = [json.dumps(article_dict, ensure_ascii=False, separators=(',', ':')) for article_dict in all_articles_content]
                        jsonl_data_str = "\n".join(json_lines)
                        jsonl_data_bytes = jsonl_data_str.encode('utf-8')
                        logger.info(f"Đã chuyển đổi thành công sang JSON Lines, kích thước: {len(jsonl_data_bytes)} bytes.")

                        object_name_in_bucket = f"{year}/{month}/{day}/{actual_topic_name.replace(' ', '_')}.jsonl"
                        logger.info(f"Chuẩn bị tải dữ liệu JSON Lines lên MinIO bucket '{minio_bucket_name}' với object name: {object_name_in_bucket}")

                        upload_success = upload_data_to_minio(
                            s3_hook_instance=s3_hook_for_minio, # Truyền đối tượng hook
                            bucket_name=minio_bucket_name,
                            object_name=object_name_in_bucket,
                            data_bytes=jsonl_data_bytes,
                            content_type='application/x-jsonlines'
                        )

                        if upload_success:
                            logger.info(f"Đã tải thành công dữ liệu cho topic '{actual_topic_name}' ngày {crawl_date_str_format} lên MinIO.")
                        else:
                            logger.error(f"Không thể tải dữ liệu cho topic '{actual_topic_name}' ngày {crawl_date_str_format} lên MinIO.")
                    else:
                        logger.info(f"Không có nội dung bài báo nào được thu thập cho topic '{actual_topic_name}' ngày {crawl_date_str_format}. Không có gì để tải lên MinIO.")
                except Exception as e:
                    logger.error(f"Lỗi không mong muốn khi xử lý category ID {category_id} (Topic: {topic_name_from_map}) cho ngày {crawl_date_str_format}: {e}", exc_info=True)
                logger.info(f"--- Hoàn thành thu thập cho Category ID: {category_id} (Topic: {topic_name_from_map}) ---")
            current_date += datetime.timedelta(days=1)
        logger.info(f"Hoàn tất quá trình xử lý cho khoảng ngày từ {start_date_str} đến {end_date_str}.")
    except Exception as main_err:
        logger.error(f"Lỗi nghiêm trọng trong hàm process_date_range_and_categories: {main_err}", exc_info=True)
    finally:
        if driver:
            driver.quit()
            logger.info("Đã đóng Selenium WebDriver.")


