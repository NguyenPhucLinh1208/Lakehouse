from selenium.webdriver.common.by import By # xác định cách thức tìm kiếm phần tử trên trang web
from selenium.webdriver.support.ui import WebDriverWait # chờ cho một điều kiện nào đó xảy ra
from selenium.webdriver.support import expected_conditions as EC # xác định điều kiện cụ thể
from selenium.webdriver.common.keys import Keys # gửi phím đến trình duyệt
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, JavascriptException
from selenium.common.exceptions import TimeoutException
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def switch_to_analysis_iframe(driver, XPATH_IFRAME):
    """
    Chuyển sang iframe chứa dữ liệu cần thu thập
    """
    # Chờ cho iframe xuất hiện
    iframe = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, f"{XPATH_IFRAME}"))
    )
    driver.switch_to.frame(iframe)  # Chuyển sang iframe

def click_element(driver, XPATH_ELEMENT):
    """
    Click vào một phần tử trên trang web
    """
    # Chờ cho phần tử xuất hiện
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, f"{XPATH_ELEMENT}"))
    )
    element.click()  # Click vào phần tử

def enter_text(driver, XPATH_ELEMENT, text):
    """
    Nhập dữ liệu vào một phần tử trên trang web
    """
    # Chờ cho phần tử xuất hiện
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, f"{XPATH_ELEMENT}"))
    )
    element.send_keys(Keys.CONTROL + "a")  # Chọn toàn bộ nội dung trong phần tử
    element.send_keys(Keys.BACKSPACE)  # Xóa nội dung trong phần tử
    element.send_keys(text)  # Nhập dữ liệu vào phần tử

def get_text(driver, XPATH_ELEMENT):
    """
    Lấy dữ liệu từ một phần tử trên trang web
    """
    # Chờ cho phần tử xuất hiện
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, f"{XPATH_ELEMENT}"))
    )
    return element.text  # Trả về dữ liệu của phần tử


def scroll_to_bottom_infinite(driver: WebDriver, scroll_pause_timeout: float = 3.0, max_attempts: int = 10):
    """
    Cuộn xuống cuối trang liên tục, sử dụng WebDriverWait để chờ nội dung mới tải

    Args:
        driver: WebDriver instance.
        scroll_pause_timeout: Thời gian tối đa (giây) chờ đợi chiều cao trang
                               tăng lên SAU MỖI LẦN CUỘN. Nếu chiều cao không
                               tăng trong khoảng thời gian này, coi như đã ổn định.
        max_attempts: Số lần thử cuộn và chờ tối đa để tránh vòng lặp vô hạn
                      nếu trang web có vấn đề hoặc tải mãi không ngừng.
    """
    print("Đang cuộn xuống cuối trang (infinite - dynamic wait)...")
    # Lấy chiều cao ban đầu
    try:
        last_height = driver.execute_script(
            "return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight)"
        )
    except Exception as e:
        print(f"Lỗi khi lấy chiều cao ban đầu: {e}. Không thể cuộn.")
        return 

    attempts = 0
    while attempts < max_attempts:
        attempts += 1
        print(f"  [Lần thử {attempts}/{max_attempts}] Chiều cao hiện tại: {last_height}. Đang cuộn xuống...")

        # 1. Thực hiện cuộn xuống dưới cùng của chiều cao hiện tại
        try:
            driver.execute_script(
                "window.scrollTo(0, Math.max(document.body.scrollHeight, document.documentElement.scrollHeight));"
            )
        except Exception as e:
             print(f"  Lỗi khi thực hiện cuộn: {e}. Dừng cuộn.")
             break # Dừng nếu không thể thực hiện script cuộn

        # 2. Chờ đợi chiều cao trang tăng lên một cách linh hoạt
        try:
            
            current_last_height = last_height
            condition = lambda d: d.execute_script(
                "return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight)"
            ) > current_last_height

            # Thực hiện chờ với timeout cho lần thử này
            print(f"  -- Chờ tối đa {scroll_pause_timeout}s để chiều cao tăng lên...")
            WebDriverWait(driver, scroll_pause_timeout).until(condition)

            # --- Nếu chờ thành công (không bị TimeoutException) ---
            
            new_height = driver.execute_script(
                "return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight)"
            )
            print(f"  --> Chiều cao đã tăng lên: {new_height}")
            last_height = new_height # Cập nhật chiều cao để dùng cho lần lặp tiếp theo

        except TimeoutException: 
            print(f"  -- Chiều cao không tăng sau {scroll_pause_timeout}s. Đã đến cuối hoặc ngừng tải. Dừng.")
            break 

        except Exception as e:
            print(f"  Lỗi không mong muốn trong quá trình chờ: {e}. Dừng cuộn.")
            break

    if attempts == max_attempts and not isinstance(e, TimeoutException): 
         print(f"  (!) Đã đạt số lần thử tối đa ({max_attempts}). Dừng cuộn.")

    print(f"Đã cuộn xong (infinite - dynamic wait) sau {attempts} lần thử.")








