# mflix_mongodb_source/__init__.py

# Import các thành phần chính từ các module trong package này
# để người dùng có thể import trực tiếp từ tên package.
# Ví dụ: from mflix_mongodb_source import mflix_main_source

from .resources import mflix_main_source, get_movies_resource, get_comments_resource, get_users_resource, get_theaters_resource

print("mflix_mongodb_source package loaded.")

# Có thể thêm các biến hoặc hàm khởi tạo khác nếu cần ở đây.
