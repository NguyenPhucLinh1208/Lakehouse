from selenium.webdriver.common.by import By # xác định cách thức tìm kiếm phần tử trên trang web
from selenium.webdriver.support.ui import WebDriverWait # chờ cho một điều kiện nào đó xảy ra
from selenium.webdriver.support import expected_conditions as EC # xác định điều kiện cụ thể
from selenium.webdriver.common.keys import Keys # gửi phím đến trình duyệt
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