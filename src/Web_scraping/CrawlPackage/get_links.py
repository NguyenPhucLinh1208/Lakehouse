import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import datetime
import time
import signal
import sys
import logging

from selenium.webdriver.common.by import By 
from selenium.common.exceptions import ( 
    NoSuchElementException,
    TimeoutException, 
    StaleElementReferenceException,
    WebDriverException
)


from SeleniumPackage import (
    init_debug_driver, 
    init_driver,
    click_element,
    scroll_to_bottom_infinite    
)

logger = logging.getLogger(__name__)

# --- Cấu hình Selectors  ---
XPATH_LOAD_MORE_BUTTON = '//*[@id="admWrapsite"]/div[3]/div[3]/div/div[1]/div[1]/div[3]/div[2]/a'  # Nút "xem thêm"
CSS_SELECTOR_LAST_LI = 'div.list_news.timeline ul > li.tlitem:last-child'  # Mục bài viết cuối cùng được hiện thị
CSS_SELECTOR_DATE_SPAN_IN_LI_TITLE = 'p.time span[title]'  # thông tin ngày giờ đăng bài
CSS_SELECTOR_DATE_TEXT_IN_LI = 'p.time span:last-of-type'
CSS_SELECTOR_ARTICLE_ITEMS_BS = 'ul li.tlitem' # mục bài viết
CSS_SELECTOR_TITLE_TAG_BS = 'h4 a'  # Tiêu đề bài viết 


BASE_URL_VTV = 'https://vtv.vn'

_driver_instance_for_signal_handler = None

def signal_handler(sig, frame):
    logger.info("Đã nhấn Ctrl+C! Đang thực hiện dọn dẹp trước khi thoát...")
    sys.exit(0)

def parse_article_date_from_selenium_element(last_li_element) -> datetime.date | None:
    """
    Trích xuất và phân tích ngày đăng của bài báo cuối cùng trong trang hiện thị.
    Trả về ngày đăng cuối cùng bài báo, hay None nêu không tìm thấy.
    """
    last_article_date_obj = None
    try:
        last_date_element = last_li_element.find_element(By.CSS_SELECTOR, CSS_SELECTOR_DATE_SPAN_IN_LI_TITLE)
        timestamp_str = last_date_element.get_attribute('title')
        logger.debug(f"Tìm thấy timestamp từ Selenium element: '{timestamp_str}'")
        if timestamp_str:
            try:
                timestamp_str_no_tz = timestamp_str.split('+')[0].split('Z')[0]
                if '.' in timestamp_str_no_tz and len(timestamp_str_no_tz.split('.')[-1]) > 6:
                    timestamp_str_no_tz = timestamp_str_no_tz.rsplit('.', 1)[0] + '.' + timestamp_str_no_tz.split('.')[-1][:6]
                last_article_datetime = datetime.datetime.fromisoformat(timestamp_str_no_tz)
                last_article_date_obj = last_article_datetime.date()
            except ValueError:
                logger.warning(f"Timestamp '{timestamp_str}' không đúng định dạng ISO.")
            except Exception as parse_err:
                logger.warning(f"Lỗi parse timestamp '{timestamp_str}': {parse_err}")
    except NoSuchElementException:
        logger.debug("Không tìm thấy span[title] trong Selenium element. Thử tìm text ngày.")
        try:
            last_date_element = last_li_element.find_element(By.CSS_SELECTOR, CSS_SELECTOR_DATE_TEXT_IN_LI)
            date_text = last_date_element.text.strip()
            logger.debug(f"Tìm thấy text ngày từ Selenium element: '{date_text}'")
            if date_text:
                try:
                    dt_format = '%d/%m/%Y'
                    date_part = date_text.split(" ")[0] if " " in date_text else date_text
                    dt_obj = datetime.datetime.strptime(date_part, dt_format)
                    last_article_date_obj = dt_obj.date()
                except ValueError:
                    logger.warning(f"Text ngày '{date_text}' không đúng định dạng {dt_format}.")
                except Exception as parse_err:
                    logger.warning(f"Lỗi parse text ngày '{date_text}': {parse_err}")
        except NoSuchElementException:
            logger.warning("Cũng không tìm thấy span chứa text ngày trong bài cuối cùng (Selenium element).")
    return last_article_date_obj


def scrape_vtv_articles(
    start_url: str,
    target_stop_date: datetime.date,
    selenium_hub_url: str = "http://localhost:4444/wd/hub",
    max_selenium_loops: int = 150,
    scroll_pause_time: float = 5.0
) -> list:

    global _driver_instance_for_signal_handler
    driver = None
    html_content_final = ""
    articles_data_collected = []

    logger.info(f"Bắt đầu cào từ: {start_url}")
    logger.info(f"Dừng khi bài báo cũ hơn: {target_stop_date.strftime('%d/%m/%Y')}")
    logger.info(f"Giới hạn vòng lặp Selenium: {max_selenium_loops}")

    try:
        driver = init_driver(selenium_hub_url)
        _driver_instance_for_signal_handler = driver
        logger.info(f"Driver khởi tạo từ {selenium_hub_url}.")

        driver.get(start_url)
        logger.info(f"Truy cập trang: {start_url}")
        time.sleep(5.0)

        continue_scraping = True
        current_loop = 0

        while continue_scraping and current_loop < max_selenium_loops:
            current_loop += 1
            logger.info(f"--- Vòng lặp Selenium #{current_loop}/{max_selenium_loops} ---")

            logger.debug("Cuộn trang sử dụng scroll_to_bottom_infinite...")
            scroll_to_bottom_infinite(driver, scroll_pause_time, 10) 

            should_click_load_more = True
            try:
                logger.debug("Tìm bài báo cuối cùng để kiểm tra ngày...") 
                last_li_element = driver.find_element(By.CSS_SELECTOR, CSS_SELECTOR_LAST_LI)
                last_article_date_obj = parse_article_date_from_selenium_element(last_li_element)

                if last_article_date_obj:
                    logger.debug(f"Ngày parse từ bài cuối: {last_article_date_obj.strftime('%d/%m/%Y')}")
                    if last_article_date_obj < target_stop_date:
                        logger.info(f"NGÀY DỪNG ĐẠT ĐƯỢC: Bài cuối ({last_article_date_obj.strftime('%d/%m/%Y')}) < Mục tiêu ({target_stop_date.strftime('%d/%m/%Y')}).")
                        continue_scraping = False
                        should_click_load_more = False
                    else:
                        logger.debug("Ngày bài cuối vẫn hợp lệ. Tiếp tục.")
                else:
                    logger.warning("Không xác định được ngày bài cuối từ Selenium. Mặc định tiếp tục tải.")
            except Exception as e_check_date:
                logger.error(f"Lỗi khi kiểm tra ngày bài cuối: {e_check_date}", exc_info=True)

            if continue_scraping and should_click_load_more:
                logger.debug("Nhấp nút 'Xem thêm'...")
                try:
                    click_element(driver, XPATH_LOAD_MORE_BUTTON)
                    logger.debug("Đã gọi click_element cho nút 'Xem thêm'.") 
                except Exception as e_click:
                    logger.error(f"Lỗi từ click_element hoặc sau đó: {e_click}. Dừng vòng lặp Selenium.", exc_info=True)
                    continue_scraping = False
            elif not continue_scraping:
                 logger.info("Điều kiện dừng Selenium đã kích hoạt.")

            if current_loop >= max_selenium_loops:
                logger.warning(f"Đạt giới hạn {max_selenium_loops} vòng lặp. Dừng Selenium.")
                continue_scraping = False
            
            logger.info(f"--- Kết thúc vòng lặp Selenium #{current_loop} (Tiếp tục={continue_scraping}) ---")

        logger.info("Thoát vòng lặp Selenium. Lấy mã nguồn trang...")
        if driver:
            html_content_final = driver.page_source
            logger.info("Đã lấy mã nguồn trang.")

    except WebDriverException as wde:
        logger.error(f"Lỗi WebDriver: {wde}", exc_info=True)
    except NameError as ne: 
        logger.error(f"Lỗi NameError: {ne}", exc_info=True)
    except Exception as e_selenium_global:
        logger.critical(f"Lỗi không mong muốn trong Selenium: {e_selenium_global}", exc_info=True)
    finally:
        logger.info("Vào khối finally của scrape_vtv_articles...")
        if driver:
            logger.info("Đang đóng trình duyệt...")
            try:
                driver.quit()
                _driver_instance_for_signal_handler = None 
                logger.info("Trình duyệt đã đóng.")
            except Exception as e_quit:
                logger.error(f"Lỗi khi đóng trình duyệt: {e_quit}", exc_info=True)
        else:
            logger.info("Không có driver được khởi tạo hoặc đã đóng.")

    if html_content_final:
        logger.info(f"Bắt đầu phân tích HTML, lọc bài đến ngày {target_stop_date.strftime('%d/%m/%Y')}...")
        try:
            soup = BeautifulSoup(html_content_final, 'html.parser')
            article_elements_bs = soup.select(CSS_SELECTOR_ARTICLE_ITEMS_BS)
            logger.info(f"BS4: Tìm thấy {len(article_elements_bs)} mục bài báo trong HTML.")

            processed_bs_count = 0
            added_bs_count = 0
            stopped_early_in_bs = False

            for item_element in article_elements_bs:
                processed_bs_count += 1
                article_info = {}
                article_date_obj_bs = None

                time_tag_span_bs = item_element.select_one(CSS_SELECTOR_DATE_SPAN_IN_LI_TITLE)
                if time_tag_span_bs and time_tag_span_bs.get('title'):
                    timestamp_str_bs = time_tag_span_bs.get('title')
                    try:
                        timestamp_str_bs_no_tz = timestamp_str_bs.split('+')[0].split('Z')[0]
                        if '.' in timestamp_str_bs_no_tz and len(timestamp_str_bs_no_tz.split('.')[-1]) > 6:
                             timestamp_str_bs_no_tz = timestamp_str_bs_no_tz.rsplit('.', 1)[0] + '.' + timestamp_str_bs_no_tz.split('.')[-1][:6]
                        article_datetime_bs = datetime.datetime.fromisoformat(timestamp_str_bs_no_tz)
                        article_date_obj_bs = article_datetime_bs.date()
                        article_info['timestamp'] = timestamp_str_bs 
                        article_info['date_iso'] = article_date_obj_bs.isoformat()
                    except ValueError: 
                        logger.warning(f"BS4: Timestamp '{timestamp_str_bs}' không đúng ISO.")
                    except Exception as e_parse_bs_title: 
                        logger.warning(f"BS4: Lỗi parse timestamp '{timestamp_str_bs}': {e_parse_bs_title}")

                if not article_date_obj_bs: 
                    date_text_span_bs = item_element.select_one(CSS_SELECTOR_DATE_TEXT_IN_LI)
                    if date_text_span_bs and date_text_span_bs.text:
                        date_text_bs = date_text_span_bs.text.strip()
                        try:
                            dt_format_bs = '%d/%m/%Y'
                            date_part_bs = date_text_bs.split(" ")[0] if " " in date_text_bs else date_text_bs
                            dt_obj_bs = datetime.datetime.strptime(date_part_bs, dt_format_bs)
                            article_date_obj_bs = dt_obj_bs.date()
                            article_info['date_iso'] = article_date_obj_bs.isoformat()
                            article_info['timestamp_text'] = date_text_bs
                        except ValueError: 
                            logger.warning(f"BS4: Text ngày '{date_text_bs}' không đúng định dạng.")
                        except Exception as e_parse_bs_text: 
                            logger.warning(f"BS4: Lỗi parse text ngày '{date_text_bs}': {e_parse_bs_text}")

                if article_date_obj_bs:
                    if article_date_obj_bs >= target_stop_date:
                        title_tag_bs = item_element.select_one(CSS_SELECTOR_TITLE_TAG_BS)
                        if title_tag_bs:
                            article_info['title'] = title_tag_bs.get('title', title_tag_bs.text.strip())
                            relative_url = title_tag_bs.get('href')
                            article_info['url'] = urljoin(BASE_URL_VTV, relative_url) if relative_url else None
                            if article_info.get('title') and article_info.get('url'):
                                articles_data_collected.append(article_info)
                                added_bs_count += 1
                        else: 
                            logger.warning(f"BS4: Mục {processed_bs_count}: Không tìm thấy thẻ tiêu đề.")
                    else:
                        logger.info(f"BS4: Dừng sớm - Bài ({article_date_obj_bs.strftime('%d/%m/%Y')}) cũ hơn mục tiêu.")
                        stopped_early_in_bs = True
                        break 
                else: 
                    logger.warning(f"BS4: Mục {processed_bs_count}: Không xác định được ngày.")
               
            logger.info(f"BS4: Đã xử lý {processed_bs_count}/{len(article_elements_bs)} mục.")
            if stopped_early_in_bs: 
                logger.info("BS4: Dừng sớm do gặp bài cũ hơn ngày cần lấy.")
            logger.info(f"BS4: Thêm được {added_bs_count} bài hợp lệ.")

        except Exception as e_bs4_global:
            logger.error(f"Lỗi không mong muốn trong BeautifulSoup: {e_bs4_global}", exc_info=True)
    else:
        logger.info("Không có nội dung HTML để phân tích bằng BeautifulSoup.")
    return articles_data_collected
           




        

                

