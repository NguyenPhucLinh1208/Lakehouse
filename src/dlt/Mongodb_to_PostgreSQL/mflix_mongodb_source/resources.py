import dlt
from pymongo import MongoClient
from bson import ObjectId # Import cần thiết

# --- Hàm helper để chuyển đổi ObjectId ---
def convert_objectids_to_str(item):
    """
    Hàm đệ quy để duyệt qua cấu trúc dữ liệu (dict, list)
    và chuyển đổi tất cả các giá trị ObjectId thành chuỗi.
    """
    if isinstance(item, ObjectId):
        return str(item)
    elif isinstance(item, dict):
        # Nếu là dict, duyệt qua các key-value và gọi đệ quy cho value
        return {key: convert_objectids_to_str(value) for key, value in item.items()}
    elif isinstance(item, list):
        # Nếu là list, duyệt qua các phần tử và gọi đệ quy cho từng phần tử
        return [convert_objectids_to_str(element) for element in item]
    else:
        # Giữ nguyên các kiểu dữ liệu khác (str, int, float, bool, None, ...)
        return item

# --- Định nghĩa các Resource Generators ---
# Best Practice: Chia nhỏ logic lấy dữ liệu cho từng collection/endpoint thành các hàm riêng biệt.
# Các hàm này thường được đánh dấu @dlt.resource.

@dlt.resource(name="movies", primary_key="_id", write_disposition="merge")
def get_movies_resource(
    mongodb_connect: str = dlt.secrets.value,
    database_name: str = dlt.config.value,
    batch_size: int = dlt.config.value,
    # Sử dụng incremental helper của DLT
    last_movie_id = dlt.sources.incremental("_id", initial_value=None) # Lấy _id lớn nhất từ lần chạy trước
):
    """Resource generator for the 'movies' collection with incremental loading."""
    print(f"Resource 'movies': Loading incrementally from _id > {last_movie_id.last_value}")
    client = MongoClient(mongodb_connect)
    db = client[database_name]
    collection = db["movies"]

    # Xây dựng query dựa trên incremental state
    query = {"_id": {"$gt": last_movie_id.last_value}} if last_movie_id.last_value else {}

    cursor = collection.find(query, batch_size=batch_size).sort("_id", 1) # Sort để đảm bảo thứ tự

    # SỬA LỖI: Dùng hàm đệ quy để chuyển đổi tất cả ObjectId thành string
    for document in cursor:
        yield convert_objectids_to_str(document)


@dlt.resource(name="comments", primary_key="_id", write_disposition="merge")
def get_comments_resource(
    mongodb_connect: str = dlt.secrets.value,
    database_name: str = dlt.config.value,
    batch_size: int = dlt.config.value,
    last_comment_id = dlt.sources.incremental("_id", initial_value=None)
):
    """Resource generator for the 'comments' collection with incremental loading."""
    print(f"Resource 'comments': Loading incrementally from _id > {last_comment_id.last_value}")
    client = MongoClient(mongodb_connect)
    db = client[database_name]
    collection = db["comments"]
    query = {"_id": {"$gt": last_comment_id.last_value}} if last_comment_id.last_value else {}

    cursor = collection.find(query, batch_size=batch_size).sort("_id", 1)

    # SỬA LỖI: Dùng hàm đệ quy để chuyển đổi tất cả ObjectId thành string
    for document in cursor:
        yield convert_objectids_to_str(document)

@dlt.resource(name="users", primary_key="_id", write_disposition="merge")
def get_users_resource(
    mongodb_connect: str = dlt.secrets.value,
    database_name: str = dlt.config.value,
    batch_size: int = dlt.config.value,
    last_user_id = dlt.sources.incremental("_id", initial_value=None)
):
    """Resource generator for the 'users' collection with incremental loading."""
    print(f"Resource 'users': Loading incrementally from _id > {last_user_id.last_value}")
    client = MongoClient(mongodb_connect)
    db = client[database_name]
    collection = db["users"]
    query = {"_id": {"$gt": last_user_id.last_value}} if last_user_id.last_value else {}

    cursor = collection.find(query, batch_size=batch_size).sort("_id", 1)

    # SỬA LỖI: Dùng hàm đệ quy để chuyển đổi tất cả ObjectId thành string
    for document in cursor:
        yield convert_objectids_to_str(document)

@dlt.resource(name="theaters", primary_key="theaterId", write_disposition="merge")
def get_theaters_resource(
    mongodb_connect: str = dlt.secrets.value,
    database_name: str = dlt.config.value,
    batch_size: int = dlt.config.value
    # Giả sử collection này nhỏ, không cần incremental hoặc _id không phù hợp
    # Lưu ý primary_key là 'theaterId' thay vì '_id'
):
    """Resource generator for the 'theaters' collection (full load example)."""
    print("Resource 'theaters': Loading full collection.")
    client = MongoClient(mongodb_connect)
    db = client[database_name]
    collection = db["theaters"]

    cursor = collection.find(batch_size=batch_size)

    # SỬA LỖI: Dùng hàm đệ quy để chuyển đổi tất cả ObjectId thành string (cho chắc chắn)
    for document in cursor:
        yield convert_objectids_to_str(document)


# --- Định nghĩa Source Function ---
# Hàm này sẽ nhóm các resource generators lại.
# Nó cũng nhận cấu hình từ .dlt/*.toml hoặc env vars.

# Đặt tên source khớp với section trong .dlt/*.toml: [sources.mflix_mongodb]
@dlt.source(name="mflix_mongodb")
def mflix_main_source(
    collections: list[str] = dlt.config.value # Lấy danh sách collections từ config
):
    """
    The main DLT source function for MFlix data. It yields selected resource generators.

    Args:
        collections_to_load (list[str]): List of collection names to load, read from
                                         config '[sources.mflix_mongodb].collections'.
    """
    print(f"Source '{mflix_main_source.name}' activated.")
    print(f"Collections requested in config: {collections}")

    available_resources = {
        "movies": get_movies_resource,
        "comments": get_comments_resource,
        "users": get_users_resource,
        "theaters": get_theaters_resource,
    }

    # Yield chỉ các resources được yêu cầu trong config
    resources_to_yield = []
    for collection_name in collections:
        if collection_name in available_resources:
            print(f"Yielding resource generator for: {collection_name}")
            resources_to_yield.append(available_resources[collection_name]()) # Gọi hàm resource để tạo generator
        else:
            print(f"Warning: Resource for collection '{collection_name}' is not defined or available. Skipping.")

    if not resources_to_yield:
         print("Warning: No resources selected to run based on config. Check '[sources.mflix_mongodb].collections' in config.toml")

    return resources_to_yield


