from SeleniumPackage import (
    init_driver,
    click_element
)
from CrawlPackage import (
    create_url_vnexpress,
    next_page_status,
    get_links_VnExpress,
    get_content_article
)
import json
import logging
import sys

log_format = "%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
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
        return []

    logger.info(f"Bắt đầu hàm scrape_vnexpress_articles với category_id: {category_id} (Topic: {determined_topic_name}), start_date: {start_date_str}, end_date: {end_date_str}")

    try:
        url = create_url_vnexpress(category_id, start_date_str, end_date_str)
        if not url:
            logger.error("Không thể tạo URL. Vui lòng kiểm tra lại các tham số đầu vào.")
            return []
        logger.info(f"Đã tạo URL thành công: {url}")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình tạo URL (create_url_vnexpress): {e}")
        return []

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
        return []

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
    return article_contents

if __name__ == "__main__":
    logger.info("Bắt đầu script lấy bài báo...")
    selenium_hub_url: str = "http://localhost:4444/wd/hub"

    driver = init_driver(selenium_hub_url)

    if driver:
        target_category_id = 5
        
        all_articles_content = scrape_vnexpress_articles(
            driver=driver,
            category_id=target_category_id,
            start_date_str="20250509",
            end_date_str="20250509"
        )

        if all_articles_content:
            output_file = f"content_VnExpress_cat{target_category_id}.json"
            with open(output_file, "w", encoding="utf-8") as f_json:
                json.dump(all_articles_content, f_json, ensure_ascii=False, indent=4)
            logger.info(f"Đã lưu {len(all_articles_content)} bài báo vào file {output_file}")
        else:
            logger.info(f"Không có nội dung bài báo nào được thu thập cho category_id {target_category_id}.")

        driver.quit()
    else:
        logger.error("Không thể khởi tạo Selenium WebDriver. Script kết thúc.")

    logger.info("Script lấy bài báo hoàn thành!")
