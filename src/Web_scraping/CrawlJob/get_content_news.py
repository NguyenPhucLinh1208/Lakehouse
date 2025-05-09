import requests
from bs4 import BeautifulSoup
import json

# URL cần tải
url = "https://vtv.vn/chinh-tri/hoa-hop-dan-toc-gan-ket-cong-dong-cho-su-nghiep-chung-20250428213333408.htm"

# Gửi request đến URL
response = requests.get(url)

# Kiểm tra nếu tải thành công
if response.status_code == 200:
    soup = BeautifulSoup(response.text, "html.parser")

    # Lấy tiêu đề
    title_tag = soup.find("h1", class_="title_detail")
    title = title_tag.get_text(strip=True) if title_tag else ""

    # Lấy tác giả
    author_tag = soup.find("p", class_="author")
    author = author_tag.get_text(strip=True).split("-")[0] if author_tag else ""

    # Lấy tóm tắt
    summary_tag = soup.find("h2", class_="sapo")
    summary = summary_tag.get_text(strip=True) if summary_tag else ""

    # Lấy nội dung
    content_tag = soup.find("div", id="entry-body")
    content = ""
    if content_tag:
        paragraphs = content_tag.find_all("p")
        clean_paragraphs = []
        for p in paragraphs:
            text = p.get_text(strip=True)
            # Bỏ các đoạn không liên quan
            if text.startswith("VTV.vn - Thời báo VTV trân trọng giới thiệu") or text.startswith("* Mời quý độc giả theo dõi"):
                continue
            clean_paragraphs.append(text)
        content = "\n\n".join(clean_paragraphs)

    # Tạo dict dữ liệu
    data = {
        "title": title,
        "author": author,
        "summary": summary,
        "content": content
    }

    # Lưu vào file JSON
    with open("article.json", "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

    print("Đã lưu thành công vào article.json")
else:
    print(f"Không thể tải trang. Mã lỗi: {response.status_code}")

