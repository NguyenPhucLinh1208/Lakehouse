from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()

minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
nessie_uri = os.getenv("NESSIE_URI")
nessie_default_branch = os.getenv("NESSIE_DEFAULT_BRANCH")

clean_catalog_name = "nessie-clean-news"
clean_catalog_warehouse_path = "s3a://clean-news-lakehouse/nessie_clean_news_warehouse"
CLEAN_DATABASE_NAME = "news_clean_db"

app_name = "IcebergNessieCleanNewsSchema"

spark_builder = SparkSession.builder.appName(app_name)

spark_builder = spark_builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark_builder = spark_builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")

spark_builder = spark_builder.config(f"spark.sql.catalog.{clean_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{clean_catalog_name}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config(f"spark.sql.catalog.{clean_catalog_name}.uri", nessie_uri) \
    .config(f"spark.sql.catalog.{clean_catalog_name}.ref", nessie_default_branch) \
    .config(f"spark.sql.catalog.{clean_catalog_name}.warehouse", clean_catalog_warehouse_path) \
    .config(f"spark.sql.catalog.{clean_catalog_name}.authentication.type", "NONE")

spark = spark_builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("SparkSession đã được khởi tạo cho việc thiết lập vùng Clean.")
print(f"Cấu hình cho Spark Catalog '{clean_catalog_name}':")
print(f"  Implementation: {spark.conf.get(f'spark.sql.catalog.{clean_catalog_name}.catalog-impl')}")
print(f"  Nessie Tool URI: {spark.conf.get(f'spark.sql.catalog.{clean_catalog_name}.uri')}")
print(f"  Warehouse Path: {spark.conf.get(f'spark.sql.catalog.{clean_catalog_name}.warehouse')}")

try:
    print(f"Đang tạo database/namespace '{CLEAN_DATABASE_NAME}' trong catalog '{clean_catalog_name}'...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{clean_catalog_name}`.`{CLEAN_DATABASE_NAME}`")
    print(f"Database/namespace `{clean_catalog_name}`.`{CLEAN_DATABASE_NAME}` đã được tạo (hoặc đã tồn tại).")
except Exception as e:
    print(f"Lỗi khi tạo database/namespace `{clean_catalog_name}`.`{CLEAN_DATABASE_NAME}`: {e}")

def create_iceberg_table_in_db(
    table_name,
    schema_fields,
    table_comment,
    db_name,
    catalog_name=clean_catalog_name,
    partitioned_by_sql=None
):
    full_table_name_in_catalog = f"`{catalog_name}`.`{db_name}`.`{table_name}`"
    print(f"Đang tạo bảng {full_table_name_in_catalog} với comments...")

    schema_sql_parts = []
    for field in schema_fields:
        col_def = f"`{field['name']}` {field['type']}"
        if field.get('comment'):
            escaped_comment = field['comment'].replace("'", "''")
            col_def += f" COMMENT '{escaped_comment}'"
        schema_sql_parts.append(col_def)
    schema_sql_str = ",\n        ".join(schema_sql_parts)

    try:
        escaped_table_comment = table_comment.replace("'", "''")
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name_in_catalog} (
            {schema_sql_str}
        ) USING iceberg
        COMMENT '{escaped_table_comment}'
        """
        if partitioned_by_sql:
            create_sql += f"\nPARTITIONED BY ({partitioned_by_sql})"
        create_sql += ";"
        
        # print(f"Executing SQL for {full_table_name_in_catalog}:\n{create_sql}") # Bỏ comment nếu muốn debug SQL
        spark.sql(create_sql)
        print(f"Bảng {full_table_name_in_catalog} đã được tạo (hoặc đã tồn tại) với comments.")
        print(f"Mô tả schema cho bảng {full_table_name_in_catalog}:")
        spark.sql(f"DESCRIBE {full_table_name_in_catalog}").show(truncate=False)
    except Exception as e:
        print(f"Lỗi khi tạo bảng {full_table_name_in_catalog}: {e}")

authors_fields = [
    {"name": "AuthorID", "type": "STRING", "comment": "Khóa chính nhân tạo (surrogate key) cho tác giả, tạo bằng hàm băm SHA256 từ AuthorName."},
    {"name": "AuthorName", "type": "STRING", "comment": "Tên của tác giả, lấy từ trường `tac_gia` của dữ liệu thô và đã được chuẩn hóa (distinct)."}
]
authors_table_comment = "Lưu trữ thông tin về các tác giả duy nhất của bài báo. Dữ liệu được chuẩn hóa từ trường `tac_gia`."
create_iceberg_table_in_db("authors", authors_fields, authors_table_comment, CLEAN_DATABASE_NAME)

topics_fields = [
    {"name": "TopicID", "type": "STRING", "comment": "Khóa chính nhân tạo cho chủ đề, tạo bằng hàm băm SHA256 từ TopicName."},
    {"name": "TopicName", "type": "STRING", "comment": "Tên của chủ đề chính, lấy từ trường `topic` của dữ liệu thô và đã được chuẩn hóa (distinct)."}
]
topics_table_comment = "Lưu trữ các chủ đề chính duy nhất của bài báo. Dữ liệu được chuẩn hóa từ trường `topic`."
create_iceberg_table_in_db("topics", topics_fields, topics_table_comment, CLEAN_DATABASE_NAME)

subtopics_fields = [
    {"name": "SubTopicID", "type": "STRING", "comment": "Khóa chính nhân tạo cho chủ đề phụ, tạo bằng hàm băm SHA256 từ SubTopicName và TopicID của chủ đề cha."},
    {"name": "SubTopicName", "type": "STRING", "comment": "Tên của chủ đề phụ, lấy từ trường `sub_topic` của dữ liệu thô."},
    {"name": "TopicID", "type": "STRING", "comment": "Khóa ngoại tham chiếu đến TopicID trong bảng `topics`, xác định chủ đề cha cho chủ đề phụ này."}
]
subtopics_table_comment = "Lưu trữ các chủ đề phụ duy nhất của bài báo, liên kết với chủ đề chính. Dữ liệu được chuẩn hóa từ `sub_topic` và `topic`."
create_iceberg_table_in_db("subtopics", subtopics_fields, subtopics_table_comment, CLEAN_DATABASE_NAME)

keywords_fields = [
    {"name": "KeywordID", "type": "STRING", "comment": "Khóa chính nhân tạo cho từ khóa, tạo bằng hàm băm SHA256 từ KeywordText."},
    {"name": "KeywordText", "type": "STRING", "comment": "Nội dung của từ khóa. Dữ liệu được chuẩn hóa sau khi `explode` từ mảng `tu_khoa` của dữ liệu thô và lấy giá trị distinct."}
]
keywords_table_comment = "Lưu trữ danh mục các từ khóa duy nhất được tìm thấy trong các bài báo."
create_iceberg_table_in_db("keywords", keywords_fields, keywords_table_comment, CLEAN_DATABASE_NAME)

references_table_fields = [
    {"name": "ReferenceID", "type": "STRING", "comment": "Khóa chính nhân tạo cho nguồn tham khảo, tạo bằng hàm băm SHA256 từ ReferenceText."},
    {"name": "ReferenceText", "type": "STRING", "comment": "Nội dung/tên của nguồn tham khảo. Dữ liệu được chuẩn hóa sau khi `explode` từ mảng `tham_khao` của dữ liệu thô và lấy giá trị distinct."}
]
references_table_comment = "Lưu trữ danh mục các nguồn tham khảo duy nhất được trích dẫn trong các bài báo."
create_iceberg_table_in_db("references_table", references_table_fields, references_table_comment, CLEAN_DATABASE_NAME)

articles_fields = [
    {"name": "ArticleID", "type": "STRING", "comment": "Khóa chính nhân tạo cho bài báo, tạo bằng hàm băm SHA256 từ trường URL duy nhất của bài báo."},
    {"name": "Title", "type": "STRING", "comment": "Tiêu đề của bài báo. Lấy từ trường `title` của dữ liệu thô."},
    {"name": "URL", "type": "STRING", "comment": "Đường dẫn URL gốc, duy nhất của bài báo. Lấy từ trường `url` của dữ liệu thô."},
    {"name": "Description", "type": "STRING", "comment": "Mô tả ngắn gọn của bài báo. Lấy từ trường `description` của dữ liệu thô."},
    {"name": "PublicationDate", "type": "TIMESTAMP", "comment": "Ngày giờ bài báo được xuất bản. Chuyển đổi từ trường `ngay_xuat_ban` của dữ liệu thô. Dùng để phân vùng bảng này."},
    {"name": "MainContent", "type": "STRING", "comment": "Nội dung chính của bài báo. Lấy từ trường `noi_dung_chinh` của dữ liệu thô."},
    {"name": "OpinionCount", "type": "INT", "comment": "Số lượng ý kiến ban đầu (nếu có) được báo cáo cho bài báo. Chuyển đổi từ trường `y_kien`, mặc định là 0 nếu giá trị không hợp lệ hoặc thiếu."},
    {"name": "AuthorID", "type": "STRING", "comment": "Khóa ngoại tham chiếu đến AuthorID trong bảng `authors`. Liên kết dựa trên `tac_gia` và `AuthorName`."},
    {"name": "TopicID", "type": "STRING", "comment": "Khóa ngoại tham chiếu đến TopicID trong bảng `topics`. Liên kết dựa trên `topic` và `TopicName`."},
    {"name": "SubTopicID", "type": "STRING", "comment": "Khóa ngoại tham chiếu đến SubTopicID trong bảng `subtopics`. Có thể là NULL nếu bài báo không có chủ đề phụ hoặc không tìm thấy mapping."}
]
articles_table_comment = "Bảng trung tâm lưu trữ thông tin chi tiết đã được làm sạch và chuẩn hóa của từng bài báo."
create_iceberg_table_in_db("articles", articles_fields, articles_table_comment, CLEAN_DATABASE_NAME, partitioned_by_sql="days(PublicationDate)")

article_keywords_fields = [
    {"name": "ArticleID", "type": "STRING", "comment": "Khóa ngoại tham chiếu đến ArticleID trong bảng `articles`."},
    {"name": "KeywordID", "type": "STRING", "comment": "Khóa ngoại tham chiếu đến KeywordID trong bảng `keywords`."}
]
article_keywords_table_comment = "Bảng nối thể hiện mối quan hệ nhiều-nhiều giữa bài báo và từ khóa, đảm bảo các cặp ArticleID-KeywordID là duy nhất."
create_iceberg_table_in_db("article_keywords", article_keywords_fields, article_keywords_table_comment, CLEAN_DATABASE_NAME)

article_references_fields = [
    {"name": "ArticleID", "type": "STRING", "comment": "Khóa ngoại tham chiếu đến ArticleID trong bảng `articles`."},
    {"name": "ReferenceID", "type": "STRING", "comment": "Khóa ngoại tham chiếu đến ReferenceID trong bảng `references_table`."}
]
article_references_table_comment = "Bảng nối thể hiện mối quan hệ nhiều-nhiều giữa bài báo và nguồn tham khảo, đảm bảo các cặp ArticleID-ReferenceID là duy nhất."
create_iceberg_table_in_db("article_references", article_references_fields, article_references_table_comment, CLEAN_DATABASE_NAME)

comments_fields = [
    {"name": "CommentID", "type": "STRING", "comment": "Khóa chính nhân tạo cho bình luận, tạo bằng hàm băm SHA256 từ RawRecordUID (ID tạm thời của bản ghi thô), CommenterName và CommentContent."},
    {"name": "ArticleID", "type": "STRING", "comment": "Khóa ngoại tham chiếu đến ArticleID trong bảng `articles` mà bình luận này thuộc về."},
    {"name": "CommenterName", "type": "STRING", "comment": "Tên người bình luận. Lấy từ `nguoi_binh_luan` trong cấu trúc `top_binh_luan` của dữ liệu thô."},
    {"name": "CommentContent", "type": "STRING", "comment": "Nội dung của bình luận. Lấy từ `noi_dung` trong cấu trúc `top_binh_luan` của dữ liệu thô."},
    {"name": "TotalLikes", "type": "INT", "comment": "Tổng số lượt thích cho bình luận. Lấy từ `tong_luot_thich` trong `top_binh_luan`, mặc định là 0 nếu giá trị không hợp lệ hoặc thiếu."}
]
comments_table_comment = "Lưu trữ các bình luận của người dùng về bài báo. Dữ liệu được trích xuất, làm sạch và chuẩn hóa từ trường `top_binh_luan`."
create_iceberg_table_in_db("comments", comments_fields, comments_table_comment, CLEAN_DATABASE_NAME)

comment_interactions_fields = [
    {"name": "CommentInteractionID", "type": "STRING", "comment": "Khóa chính nhân tạo cho một loại tương tác cụ thể của bình luận, tạo bằng hàm băm SHA256 từ CommentID và InteractionType."},
    {"name": "CommentID", "type": "STRING", "comment": "Khóa ngoại tham chiếu đến CommentID trong bảng `comments`."},
    {"name": "InteractionType", "type": "STRING", "comment": "Loại tương tác (ví dụ: `Thích`, `Vui`). Lấy từ key trong map `chi_tiet_tuong_tac` của mỗi bình luận trong dữ liệu thô."},
    {"name": "InteractionCount", "type": "INT", "comment": "Số lượng của loại tương tác này. Lấy từ value trong map `chi_tiet_tuong_tac`, mặc định là 0 nếu giá trị không hợp lệ hoặc thiếu."}
]
comment_interactions_table_comment = "Lưu trữ số lượng chi tiết cho từng loại tương tác của mỗi bình luận. Dữ liệu được `explode` từ map `chi_tiet_tuong_tac`."
create_iceberg_table_in_db("comment_interactions", comment_interactions_fields, comment_interactions_table_comment, CLEAN_DATABASE_NAME)

print(f"Hoàn tất việc tạo/cập nhật schema các bảng trong database '{CLEAN_DATABASE_NAME}' của catalog '{clean_catalog_name}'.")
spark.stop()
