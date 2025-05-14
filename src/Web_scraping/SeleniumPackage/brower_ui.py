from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def init_debug_driver(chrome_url):
    # Khởi tạo tùy chọn cho Chrome
    chrome_options = Options()
    # === CÀI ĐẶT PAGE LOAD STRATEGY ===
    # 'normal': (Mặc định) Chờ toàn bộ trang tải xong.
    # 'eager': Chờ DOM sẵn sàng (HTML được tải và phân tích), không chờ tài nguyên phụ.
    # 'none': Trả về ngay sau khi nhận được HTML ban đầu.
    chrome_options.page_load_strategy = 'eager'  # HOẶC 'none'    
    # Các tùy chọn cơ bản cho ổn định khi chạy trong Docker (nếu cần)
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")  # Cửa sổ trình duyệt kích thước cố định
    
    # Tùy chọn để tránh bị phát hiện là bot tự động
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Thiết lập user-agent để giả lập trình duyệt người dùng thực
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    chrome_options.add_argument(f"user-agent={user_agent}")
    
    # Loại bỏ chế độ headless để chạy giao diện
    # (Không thêm "--headless" giúp cửa sổ trình duyệt được hiển thị)
    
    # Cho phép tải hình ảnh và fonts để debug chính xác giao diện
    prefs = {
        "profile.managed_default_content_settings.images": 1,  # 1: load, 2: tắt
        "profile.managed_default_content_settings.fonts": 1,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Tùy chọn bổ sung cho debug: bật logging chi tiết
    chrome_options.add_argument("--enable-logging")
    chrome_options.add_argument("--v=1")
    
    # Khởi tạo Selenium WebDriver kết nối đến Selenium Server (hoặc container)
    driver = webdriver.Remote(
        command_executor=chrome_url,  # Địa chỉ Selenium Server
        options=chrome_options
    )
    return driver

