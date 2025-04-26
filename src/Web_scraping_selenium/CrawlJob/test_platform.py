from ScarpingPackage.brower_ui import init_debug_driver
from ScarpingPackage.brower_no_ui import init_driver
from ScarpingPackage.navigation import click_element, enter_text, get_text
import time

driver = init_debug_driver("http://localhost:4444/wd/hub")
driver.get("https://vtv.vn/trong-nuoc.htm")

click_element(driver, '//*[@id="admWrapsite"]/div[1]/div/div[1]/div[2]/div[1]')
enter_text(driver, '//*[@id="txtSearch"]', 'Sơn Tùng M-TP')
click_element(driver, '//*[@id="btnSearch"]')
click_element(driver, '//*[@id="SearchSolr1"]/li[1]/h4')
print(get_text(driver, '//*[@id="entry-body"]'))
driver.quit()
