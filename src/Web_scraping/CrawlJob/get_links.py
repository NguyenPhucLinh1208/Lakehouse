from CrawlPackage import signal_handler, scrape_vtv_articles
import logging
import signal
import sys
import json
import datetime

log_format = "%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s"
logging.basicConfig(
    level = logging.INFO,
    format = log_format,
    handlers = [
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

logger.info("Bắt đầu script lấy links bài báo và các thông tin tổng quan khác...")
signal.signal(signal.SIGINT, signal_handler)
logger.info("Đã đăng ký xử lý Ctrl+C.")

output_json_file = 'vtv_links.json'
page_to_scrape_url = 'https://vtv.vn/chinh-tri.htm'

try:
    TARGET_STOP_DATE_CONFIG = datetime.date(2025, 5, 5)
    logger.info(f"Ngày dừng mục tiêu (lấy bài >= ngày này): {TARGET_STOP_DATE_CONFIG.strftime('%d/%m/%Y')}")
except ValueError:
    logger.critical("Ngày dừng không hợp lệ. Thoát.", exc_info=True)
    sys.exit(1)

collected_articles = scrape_vtv_articles(
    start_url = page_to_scrape_url,
    target_stop_date = TARGET_STOP_DATE_CONFIG,
)

if collected_articles:
    logger.info(f"Tổng cộng thu thập được {len(collected_articles)} bài báo.")
    logger.info(f"Ghi vào file: '{output_json_file}'...")
    try:
        with open(output_json_file, 'w', encoding='utf-8') as f_json:
            json.dump(collected_articles, f_json, ensure_ascii=False, indent=4)
        logger.info("Ghi file JSON thành công.")
    except IOError as e_io:
        logger.critical(f"Lỗi ghi file: {e_io}", exc_info=True)
    except Exception as e:
        logger.critical(f"Lỗi JSON khác: {e}", exc_info=True)
else:
    logger.info("Không tìm thấy bài báo nào.")

logger.info("Script lấy links bài báo hoàn thành!")
