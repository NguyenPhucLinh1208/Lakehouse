from datetime import datetime, timezone
import logging
import time
import re
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.common.by import By

from selenium.common.exceptions import ( 
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException                                     
)

from Web_scraping.SeleniumPackage import (
    init_debug_driver, 
    init_driver,
    click_element
)


logger = logging.getLogger(__name__)

def create_url_vnexpress(index: int, start_date: str, end_date: str) -> str:
    """
    Tạo đường link tới VnExpress, dựa trên chuyên mục và thời gian.
    Các số đầu vào cho chuyên mục tương ứng như sau:
    1: Thời sự
    2: Góc nhìn
    3: Thế giới
    4: Kinh doanh
    5: Bất động sản
    6: Pháp luật
    7: Giáo dục
    8: Sức khỏe
    9: Đời sống
    10: Du lịch
    11: Khoa học công nghệ

    Args:
        index: Một số từ 1 đến 11 đại diện cho chuyên mục.
        start_date: Ngày bắt đầu ở định dạng 'YYYYMMDD'.
        end_date: Ngày kết thúc ở định dạng 'YYYYMMDD'.
    """
    danh_sach_muc = {
        1: {"ten": "Thời sự", "id": "1001005"},
        2: {"ten": "Góc nhìn", "id": "1003450"},
        3: {"ten": "Thế giới", "id": "1001002"},
        4: {"ten": "Kinh doanh", "id": "1003159"},
        5: {"ten": "Bất động sản", "id": "1005628"},
        6: {"ten": "Pháp luật", "id": "1001007"},
        7: {"ten": "Giáo dục", "id": "1003497"},
        8: {"ten": "Sức khỏe", "id": "1003750"},
        9: {"ten": "Đời sống", "id": "1002966"},
        10: {"ten": "Du lịch", "id": "1003231"},
        11: {"ten": "Khoa học công nghệ", "id": "1006219"},
    }

    muc_duoc_chon = danh_sach_muc.get(index)
    cate_id = muc_duoc_chon["id"]

    start_date_obj = datetime.strptime(start_date, '%Y%m%d').date()
    end_date_obj = datetime.strptime(end_date, '%Y%m%d').date()

    start_date_utc = datetime(start_date_obj.year, start_date_obj.month, start_date_obj.day, tzinfo=timezone.utc)
    end_date_utc = datetime(end_date_obj.year, end_date_obj.month, end_date_obj.day, tzinfo=timezone.utc)

    timestamp_start = int(start_date_utc.timestamp())
    timestamp_end = int(end_date_utc.timestamp())

    url_goc = "https://vnexpress.net/category/day/cateid"
    url_hoan_chinh = f"{url_goc}/{cate_id}/fromdate/{timestamp_start}/todate/{timestamp_end}"

    return url_hoan_chinh

def next_page_status(driver) -> bool:
    """
    Thực hiện cuộn xuống dưới cùng, và kiểm tra nút next_page.
    """

    try:
        driver.execute_script(
            "window.scrollTo(0, Math.max(document.body.scrollHeight, document.documentElement.scrollHeight));"
        )
    except Exception as e:
         print(f"  Lỗi khi thực hiện cuộn: {e}. Dừng cuộn.")

    target_xpath = "//a[contains(@class, 'btn-page') and contains(@class, 'next-page')]"

    try:        
        nut_tiep_theo = driver.find_element(By.XPATH, target_xpath)
    except NoSuchElementException:
        logger.info(f"Không tìm thấy nút 'Tiếp theo'. Chỉ có một trang duy nhất.")
        return False 

    try:
        if not nut_tiep_theo.is_displayed():
            logger.warning("Nút 'Tiếp theo' được tìm thấy nhưng không hiển thị.")
            return False

        href = nut_tiep_theo.get_attribute("href") or ""
        class_attr = nut_tiep_theo.get_attribute("class") or ""

        if href.strip().lower() == "javascript:;" or "disable" in class_attr.lower():
            logger.info("Đã là trang cuối...")
            return False

        logger.info("Nút 'Tiếp theo' hợp lệ...")
        return True

    except Exception as e:
        logger.error(f"Lỗi khi click nút 'Tiếp theo': {type(e).__name__} - {e}")
        return False
        
def get_links_VnExpress(html_content: str, topic: str) -> list:
    """
    Lấy danh sách các bài báo trong cùng một page
    """
    soup = BeautifulSoup(html_content, "lxml")
    articles_data = []

    for article_tag in soup.find_all('article', class_='item-news-common'):
        data = {
            "title": None,
            "description": None,
            "url": None,
            "topic": topic,
            "sub_topic": None
        }

        title_tag = article_tag.find('h3', class_='title-news')
        if title_tag and title_tag.a:
            data['title'] = title_tag.a.get_text(strip=True)
            data['url'] = title_tag.a.get('href')

        description_tag = article_tag.find('p', class_='description')
        if description_tag and description_tag.a:
            data['description'] = description_tag.a.get_text(strip=True)
            if not data['url'] and description_tag.a.get('href'): 
                data['url'] = description_tag.a.get('href')

        meta_news_tag = article_tag.find('p', class_='meta-news')
        if meta_news_tag:
            category_tag = meta_news_tag.find('a', class_='cat') 
            if category_tag:
                data['sub_topic'] = category_tag.get_text(strip=True)

        articles_data.append(data)
    return articles_data


def get_content_article(driver, url: str) -> dict:
    """
    Lấy nội dung bài báo từ URL đã cho bằng Selenium và BeautifulSoup.

    Args:
        driver: Phiên bản Selenium WebDriver đã được khởi tạo.
        url: Địa chỉ URL của bài báo cần lấy nội dung.

    Returns:
        Một dict chứa thông tin chi tiết của bài báo.
    """
    article_info = {}

    try:
        driver.get(url)

        y_kien_xpath = '//*[@id="box_comment_vne"]/div/div[1]/div/h3' 

        try:
            WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((By.XPATH, y_kien_xpath))
            )            
            element_to_scroll_to = driver.find_element(By.XPATH, y_kien_xpath)            
            driver.execute_script("arguments[0].scrollIntoView(true);", element_to_scroll_to)
            WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, y_kien_xpath))
            )
        except TimeoutException:
            logger.info(f"Trang '{url}' không có mục bình luận.")
        except StaleElementReferenceException as e:
            logger.warning(
                f"Element bị stale khi cuộn cho URL '{url}': {e}. Thử lại hoặc bỏ qua bình luận.",
                exc_info=True
            )
        except Exception as e:
            logger.warning(
                f"Đã xảy ra lỗi không mong muốn khi cuộn cho URL '{url}': {e}. Tiếp tục mà không cuộn đến bình luận.",
                exc_info=True
            )
         
        html_content = driver.page_source

        # bắt đầu lấy dữ liệu bằng bs4

        soup = BeautifulSoup(html_content, 'lxml')

        # từ khóa
        keywords_list = []
        keywords_tag = soup.find('meta', attrs={'name': 'keywords'})
        if keywords_tag and keywords_tag.get('content'):
            keywords_list.extend([kw.strip() for kw in keywords_tag['content'].split(',')])

        news_keywords_tag = soup.find('meta', attrs={'name': 'news_keywords'})
        if news_keywords_tag and news_keywords_tag.get('content'):
            keywords_list.extend([kw.strip() for kw in news_keywords_tag['content'].split(',')])

        article_tag_meta_elements = soup.find_all('meta', property='article:tag')
        if article_tag_meta_elements:
            for tag_element in article_tag_meta_elements:
                if tag_element and tag_element.get('content'):
                    content_value = tag_element['content']
                    individual_keywords_from_tag = [kw.strip() for kw in content_value.split(',')]
                    keywords_list.extend(individual_keywords_from_tag)

        if keywords_list:
            unique_keywords = list(set([kw for kw in keywords_list if kw]))
            article_info['tu_khoa'] = unique_keywords
        else:
            article_info['tu_khoa'] = "Không tìm thấy từ khóa"

        # ngày xuất bản
        date_published_tag = soup.find('meta', itemprop='datePublished')
        if date_published_tag and date_published_tag.get('content'):
            article_info['ngay_xuat_ban'] = date_published_tag['content'].strip()
        else:
            pubdate_tag = soup.find('meta', attrs={'name': 'pubdate'})
            if pubdate_tag and pubdate_tag.get('content'):
                article_info['ngay_xuat_ban'] = pubdate_tag['content'].strip()
            else:
                article_info['ngay_xuat_ban'] = "Không tìm thấy ngày xuất bản"

        # tác giả và tham khảo
        author = "Báo VnExpress"
        references = None      
        article_end_tag = soup.find('span', id='article-end')

        if article_end_tag:

            author_paragraph = article_end_tag.find_previous_sibling('p', class_='Normal', style='text-align:right;')

            if not author_paragraph:

                possible_paragraphs = soup.find_all('p', class_='Normal', style='text-align:right;')
                if possible_paragraphs:
                    author_paragraph = possible_paragraphs[-1]

            if author_paragraph:

                strong_tag = author_paragraph.find('strong')
                if strong_tag:
                    author = strong_tag.get_text(strip=True)

                paragraph_text = author_paragraph.get_text(separator=' ', strip=True) 

                match = re.search(r'\((?:[Tt]heo\s+)?(.*?)\)', paragraph_text)

                if match:
                    references_text = match.group(1).strip() 
                    if references_text:

                        ref_soup = BeautifulSoup(f"<span>{references_text}</span>", "lxml")
                        cleaned_references_text = ref_soup.get_text(strip=True)

                        references_list = re.split(r'\s*,\s*|\s+và\s+', cleaned_references_text)
                        references = [ref.strip() for ref in references_list if ref.strip()]
                        if not references: 
                            references = None
        article_info['tac_gia'] = author
        article_info['tham_khao'] = references
        
        # Nội dung chính
        article_body = soup.find('article', class_='fck_detail')
        if not article_body:
            article_body = soup.find('div', class_='fck_detail')
        if not article_body:
            article_body = soup.find('body')
        main_content_text = []
        if article_body: 
            paragraphs = article_body.find_all('p', class_='Normal') 
            if not paragraphs: 
                paragraphs = article_body.find_all('p')

            for p in paragraphs:
                
                text_content = p.get_text(separator=' ', strip=True)
                if text_content and not (p.get('style') and 'text-align:right' in p.get('style','').lower() and len(text_content.split()) < 15):
                     if not (p.find_parent('figure') or p.find_parent(class_='image')): 
                        if p.find('strong') and p.get_text(strip=True).lower() == article_info.get('tac_gia','').lower(): 
                            continue
                        main_content_text.append(text_content)
        if main_content_text and article_info.get('tac_gia') != "Không tìm thấy tác giả":
            if main_content_text[-1].strip().lower() == article_info['tac_gia'].strip().lower():
                main_content_text.pop()

        if main_content_text:
            article_info['noi_dung_chinh'] = "\n".join(main_content_text)
        else:
            article_info['noi_dung_chinh'] = "Không thể tự động trích xuất nội dung chính từ HTML tĩnh này. Nội dung có thể được tải động hoặc cần selector chính xác hơn."
        
        # Ý kiến
        article_info['y_kien'] = "0"
        total_comment_tag = soup.find('label', id='total_comment')
        if total_comment_tag:
            count_text = total_comment_tag.get_text(strip=True)
            if count_text and count_text.isdigit():
                article_info['y_kien'] = count_text
        
        detailed_comments_list = []
        comment_items_level1 = soup.find_all('div', class_='comment_item')
        comment_items_to_loop = []

        if comment_items_level1:
            for ci in comment_items_level1:
                cc = ci.find('div', class_='content-comment')
                if cc:
                    comment_items_to_loop.append(cc)
        else:
            comment_items_to_loop = soup.select('div.content-comment:not(div.sub_comment div.content-comment)')

        for item in comment_items_to_loop:
            comment_obj = {}
            comment_text_content = ""
            commenter_name = "Không rõ"

            paragraph_for_comment_extraction = item.find('p', class_='full_content')
            if not paragraph_for_comment_extraction or not paragraph_for_comment_extraction.get_text(strip=True):
                paragraph_for_comment_extraction = item.find('p', class_='content_more')
            if not paragraph_for_comment_extraction or not paragraph_for_comment_extraction.get_text(strip=True):
                paragraph_for_comment_extraction = item.find('p', class_='content_less')

            if paragraph_for_comment_extraction:
                nickname_span_tag = paragraph_for_comment_extraction.find('span', class_='txt-name')
                if nickname_span_tag:
                    nickname_a_tag = nickname_span_tag.find('a', class_='nickname')
                    if nickname_a_tag:
                        commenter_name = nickname_a_tag.get_text(strip=True)
                
                temp_paragraph_soup = BeautifulSoup(str(paragraph_for_comment_extraction), 'lxml')
                temp_p_tag = temp_paragraph_soup.find('p')
                if temp_p_tag:
                    span_to_remove = temp_p_tag.find('span', class_='txt-name')
                    if span_to_remove: span_to_remove.decompose()
                    read_more_link_to_remove = temp_p_tag.find('a', class_='icon_show_full_comment')
                    if read_more_link_to_remove: read_more_link_to_remove.decompose()
                    for br_tag_in_temp in temp_p_tag.find_all('br'):
                        br_tag_in_temp.replace_with('\n')
                    comment_text_content = temp_p_tag.get_text(strip=True)
                else:
                    comment_text_content = "Lỗi khi xử lý thẻ p của bình luận."
            else:
                comment_text_content = "Không tìm thấy thẻ <p> chứa nội dung bình luận."
            
            comment_obj['nguoi_binh_luan'] = commenter_name
            comment_obj['noi_dung'] = comment_text_content.strip()
            
            block_like_web = item.find('div', class_='block_like_web')
            if not block_like_web:
                parent_item_wrapper = item.find_parent(class_='comment_item')
                if parent_item_wrapper:
                    block_like_web = parent_item_wrapper.find('div', class_='block_like_web')

            if block_like_web:
                total_likes_text = "0"
                reactions_summary = {}
                reactions_total_div = block_like_web.find('div', class_='reactions-total')
                if reactions_total_div:
                    number_tag = reactions_total_div.find('a', class_='number')
                    if number_tag and number_tag.string:
                        total_likes_text = number_tag.string.strip()
                    
                    reactions_detail_div = reactions_total_div.find('div', class_='reactions-detail')
                    if reactions_detail_div:
                        reaction_elements = reactions_detail_div.find_all('div', class_='item')
                        for re_item in reaction_elements:
                            img_tag = re_item.find('img', alt=True)
                            count_tag = re_item.find('strong')
                            if img_tag and count_tag and count_tag.string:
                                reactions_summary[img_tag['alt']] = count_tag.string.strip()
                        if total_likes_text == "0" and "Thích" in reactions_summary:
                             total_likes_text = reactions_summary["Thích"]
                
                comment_obj['tong_luot_thich'] = total_likes_text
                comment_obj['chi_tiet_tuong_tac'] = reactions_summary if reactions_summary else "Không có chi tiết tương tác"
            else:
                comment_obj['tong_luot_thich'] = "0"
                comment_obj['chi_tiet_tuong_tac'] = "Không có chi tiết tương tác"

            valid_content = comment_obj.get('noi_dung') and \
                            comment_obj['noi_dung'] not in ["Không tìm thấy thẻ <p> chứa nội dung bình luận.",
                                                             "Lỗi khi xử lý thẻ p của bình luận."]
            if valid_content:
                detailed_comments_list.append(comment_obj)

        if detailed_comments_list:
            article_info['top_binh_luan'] = detailed_comments_list
        else:
            article_info['top_binh_luan'] = "Không tìm thấy bình luận chi tiết hoặc bình luận được tải động."

    except Exception as e:
        logger.error(f"Đã xảy ra lỗi không mong muốn trong get_content_article cho URL '{url}': {e}", exc_info=True)
        article_info['error'] = f"An unexpected error occurred: {str(e)}"

    return article_info


