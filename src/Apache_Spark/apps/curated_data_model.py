from pyspark.sql import SparkSession

minio_endpoint = "http://minio1:9000"
from pyspark.sql import SparkSession

minio_endpoint = "http://minio1:9000"
minio_access_key = "rH4arFLYBxl55rh2zmN1"
minio_secret_key = "AtEB2XiMAznxvJXRvaXxigdIewIMVKscgPg7dJjI"
nessie_uri = "http://nessie:19120/api/v2"
nessie_default_branch = "main"

curated_catalog_name = "nessie-curated-news"
curated_catalog_warehouse_path = "s3a://curated-news-lakehouse/nessie_curated_news_warehouse"
CURATED_DATABASE_NAME = "news_curated_db"

app_name = "IcebergNessieCuratedNewsSchemaFinalPreciseComments"

spark_builder = SparkSession.builder.appName(app_name)

spark_builder = spark_builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark_builder = spark_builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")

spark_builder = spark_builder.config(f"spark.sql.catalog.{curated_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{curated_catalog_name}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config(f"spark.sql.catalog.{curated_catalog_name}.uri", nessie_uri) \
    .config(f"spark.sql.catalog.{curated_catalog_name}.ref", nessie_default_branch) \
    .config(f"spark.sql.catalog.{curated_catalog_name}.warehouse", curated_catalog_warehouse_path) \
    .config(f"spark.sql.catalog.{curated_catalog_name}.authentication.type", "NONE")

spark = spark_builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("SparkSession đã được khởi tạo cho việc thiết lập vùng Curated (phiên bản cuối cùng).")
print(f"Cấu hình cho Spark Catalog '{curated_catalog_name}':")
print(f"  Implementation: {spark.conf.get(f'spark.sql.catalog.{curated_catalog_name}.catalog-impl')}")
print(f"  Nessie Tool URI: {spark.conf.get(f'spark.sql.catalog.{curated_catalog_name}.uri')}")
print(f"  Warehouse Path: {spark.conf.get(f'spark.sql.catalog.{curated_catalog_name}.warehouse')}")

try:
    print(f"Đang tạo database/namespace '{CURATED_DATABASE_NAME}' trong catalog '{curated_catalog_name}'...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{curated_catalog_name}`.`{CURATED_DATABASE_NAME}`")
    print(f"Database/namespace `{curated_catalog_name}`.`{CURATED_DATABASE_NAME}` đã được tạo (hoặc đã tồn tại).")
except Exception as e:
    print(f"Lỗi khi tạo database/namespace `{curated_catalog_name}`.`{CURATED_DATABASE_NAME}`: {e}")

def create_iceberg_table_in_db_curated(
    table_name,
    schema_fields,
    table_comment,
    db_name,
    catalog_name=curated_catalog_name,
    partitioned_by_sql=None
):
    full_table_name_in_catalog = f"`{catalog_name}`.`{db_name}`.`{table_name}`"
    print(f"Đang tạo bảng {full_table_name_in_catalog} với comments...")

    schema_sql_parts = []
    for field in schema_fields:
        col_def = f"`{field['name']}` {field['type']}"
        if field.get('comment'):
            # Escaping single quotes for SQL string literal compatibility
            escaped_comment = field['comment'].replace("'", "''")
            col_def += f" COMMENT '{escaped_comment}'"
        schema_sql_parts.append(col_def)
    schema_sql_str = ",\n        ".join(schema_sql_parts) # Indentation for readability in generated SQL

    try:
        # Escaping single quotes in table_comment for SQL string literal compatibility
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
        
        print(f"Executing SQL for {full_table_name_in_catalog}:\n{create_sql}") # Log the SQL
        spark.sql(create_sql)
        print(f"Bảng {full_table_name_in_catalog} đã được tạo (hoặc đã tồn tại) với comments.")
        print(f"Mô tả schema cho bảng {full_table_name_in_catalog}:")
        spark.sql(f"DESCRIBE {full_table_name_in_catalog}").show(truncate=False)
    except Exception as e:
        print(f"Lỗi khi tạo bảng {full_table_name_in_catalog}: {e}")

# --- I. Bảng Chiều (Dimension Tables) ---

dim_date_fields = [
    {"name": "DateKey", "type": "INT", "comment": "Khóa chính (PK) của bảng, khóa thay thế duy nhất cho mỗi ngày (ví dụ: 20250522)."},
    {"name": "FullDateAlternateKey", "type": "DATE", "comment": "Ngày đầy đủ (ví dụ: `2025-05-22`). Khóa tự nhiên, dùng để join với dữ liệu nguồn."},
    {"name": "DayNameOfWeek", "type": "STRING", "comment": "Tên của ngày trong tuần (ví dụ: `Thứ Năm`)."},
    {"name": "DayNumberOfMonth", "type": "INT", "comment": "Ngày trong tháng (1-31)."},
    {"name": "DayNumberOfYear", "type": "INT", "comment": "Ngày trong năm (1-366)."},
    {"name": "MonthName", "type": "STRING", "comment": "Tên tháng (ví dụ: `Tháng Năm`)."},
    {"name": "MonthNumberOfYear", "type": "INT", "comment": "Tháng trong năm (1-12)."},
    {"name": "CalendarQuarter", "type": "INT", "comment": "Quý trong năm (1, 2, 3, 4)."},
    {"name": "CalendarYear", "type": "INT", "comment": "Năm theo lịch."}
]
dim_date_table_comment = "Bảng chiều cung cấp các thuộc tính thời gian để phân tích dữ liệu theo ngày, tháng, quý, năm. Cần script riêng để tạo và điền dữ liệu."
create_iceberg_table_in_db_curated("dim_date", dim_date_fields, dim_date_table_comment, CURATED_DATABASE_NAME)

dim_author_fields = [
    {"name": "AuthorKey", "type": "BIGINT", "comment": "Khóa chính (PK), khóa thay thế duy nhất cho mỗi tác giả. Được tạo bằng cách hash nhất quán từ AuthorID_NK (hoặc AuthorName) và chuyển đổi sang số nguyên BIGINT."},
    {"name": "AuthorID_NK", "type": "STRING", "comment": "Khóa tự nhiên (Natural Key) của tác giả, từ AuthorID của bảng `authors` (clean)."},
    {"name": "AuthorName", "type": "STRING", "comment": "Tên của tác giả."}
]
dim_author_table_comment = "Bảng chiều cung cấp thông tin về tác giả bài viết để phân tích hiệu suất, đóng góp. Nguồn: bảng `authors` (clean)."
create_iceberg_table_in_db_curated("dim_author", dim_author_fields, dim_author_table_comment, CURATED_DATABASE_NAME)

dim_topic_fields = [
    {"name": "TopicKey", "type": "BIGINT", "comment": "Khóa chính (PK), khóa thay thế duy nhất cho mỗi chủ đề. Được tạo bằng cách hash nhất quán từ TopicID_NK (hoặc TopicName) và chuyển đổi sang số nguyên BIGINT."},
    {"name": "TopicID_NK", "type": "STRING", "comment": "Khóa tự nhiên (Natural Key) của chủ đề, từ TopicID của bảng `topics` (clean)."},
    {"name": "TopicName", "type": "STRING", "comment": "Tên của chủ đề chính."}
]
dim_topic_table_comment = "Bảng chiều cung cấp thông tin về chủ đề chính của bài viết để phân tích xu hướng. Nguồn: bảng `topics` (clean)."
create_iceberg_table_in_db_curated("dim_topic", dim_topic_fields, dim_topic_table_comment, CURATED_DATABASE_NAME)

dim_subtopic_fields = [
    {"name": "SubTopicKey", "type": "BIGINT", "comment": "Khóa chính (PK), khóa thay thế duy nhất cho mỗi chủ đề phụ. Được tạo bằng cách hash nhất quán từ SubTopicID_NK (hoặc SubTopicName kết hợp ParentTopicKey) và chuyển đổi sang số nguyên BIGINT."},
    {"name": "SubTopicID_NK", "type": "STRING", "comment": "Khóa tự nhiên (Natural Key) của chủ đề phụ, từ SubTopicID của bảng `subtopics` (clean)."},
    {"name": "SubTopicName", "type": "STRING", "comment": "Tên của chủ đề phụ."},
    {"name": "ParentTopicKey", "type": "BIGINT", "comment": "Khóa ngoại (FK) trỏ đến dim_topic.TopicKey. Giá trị -1 nếu không có chủ đề cha hoặc không áp dụng."},
    {"name": "ParentTopicName", "type": "STRING", "comment": "Tên của chủ đề chính (denormalized từ dim_topic để tiện truy vấn)."}
]
dim_subtopic_table_comment = "Bảng chiều cung cấp thông tin chi tiết về chủ đề phụ. Nguồn: `subtopics` (clean) và join với `dim_topic` (curated)."
create_iceberg_table_in_db_curated("dim_sub_topic", dim_subtopic_fields, dim_subtopic_table_comment, CURATED_DATABASE_NAME)

dim_keyword_fields = [
    {"name": "KeywordKey", "type": "BIGINT", "comment": "Khóa chính (PK), khóa thay thế duy nhất cho mỗi từ khóa. Được tạo bằng cách hash nhất quán từ KeywordID_NK (hoặc KeywordText) và chuyển đổi sang số nguyên BIGINT."},
    {"name": "KeywordID_NK", "type": "STRING", "comment": "Khóa tự nhiên (Natural Key) của từ khóa, từ KeywordID của bảng `keywords` (clean)."},
    {"name": "KeywordText", "type": "STRING", "comment": "Nội dung của từ khóa."}
]
dim_keyword_table_comment = "Bảng chiều cung cấp danh sách các từ khóa đã được gán cho bài viết. Nguồn: bảng `keywords` (clean)."
create_iceberg_table_in_db_curated("dim_keyword", dim_keyword_fields, dim_keyword_table_comment, CURATED_DATABASE_NAME)

dim_referencesource_fields = [
    {"name": "ReferenceSourceKey", "type": "BIGINT", "comment": "Khóa chính (PK), khóa thay thế duy nhất cho mỗi nguồn tham khảo. Được tạo bằng cách hash nhất quán từ ReferenceID_NK (hoặc ReferenceText) và chuyển đổi sang số nguyên BIGINT."},
    {"name": "ReferenceID_NK", "type": "STRING", "comment": "Khóa tự nhiên (Natural Key) của nguồn tham khảo, từ ReferenceID của bảng `references_table` (clean)."},
    {"name": "ReferenceText", "type": "STRING", "comment": "Tên/nội dung của nguồn tham khảo."}
]
dim_referencesource_table_comment = "Bảng chiều cung cấp danh sách các nguồn tham khảo được trích dẫn. Nguồn: bảng `references_table` (clean)."
create_iceberg_table_in_db_curated("dim_reference_source", dim_referencesource_fields, dim_referencesource_table_comment, CURATED_DATABASE_NAME)

dim_interactiontype_fields = [
    {"name": "InteractionTypeKey", "type": "BIGINT", "comment": "Khóa chính (PK), khóa thay thế duy nhất cho mỗi loại tương tác. Được tạo bằng cách hash nhất quán từ InteractionTypeName và chuyển đổi sang số nguyên BIGINT."},
    {"name": "InteractionTypeName", "type": "STRING", "comment": "Tên của loại tương tác (ví dụ: `Thích`, `Vui`). Khóa tự nhiên, trích xuất từ giá trị duy nhất trong `comment_interactions` (clean)."}
]
dim_interactiontype_table_comment = "Bảng chiều phân loại các loại tương tác trên bình luận. Nguồn: giá trị duy nhất từ `comment_interactions` (clean)."
create_iceberg_table_in_db_curated("dim_interaction_type", dim_interactiontype_fields, dim_interactiontype_table_comment, CURATED_DATABASE_NAME)

# --- II. Bảng Sự kiện (Fact Tables) ---

fact_article_publication_fields = [
    {"name": "PublicationDateKey", "type": "INT", "comment": "Thành phần của khóa tổ hợp định danh bản ghi sự kiện. Đồng thời là Khóa ngoại (FK) trỏ đến dim_date.DateKey (chỉ phần ngày). Giá trị -1 nếu không map được."},
    {"name": "ArticlePublicationTimestamp", "type": "TIMESTAMP", "comment": "Thời điểm đầy đủ bài viết được xuất bản. Degenerate Dimension/Measure. Dùng để phân vùng."},
    {"name": "AuthorKey", "type": "BIGINT", "comment": "Thành phần của khóa tổ hợp định danh bản ghi sự kiện. Đồng thời là Khóa ngoại (FK) trỏ đến dim_author.AuthorKey. Giá trị -1 nếu không map được."},
    {"name": "TopicKey", "type": "BIGINT", "comment": "Thành phần của khóa tổ hợp định danh bản ghi sự kiện. Đồng thời là Khóa ngoại (FK) trỏ đến dim_topic.TopicKey. Giá trị -1 nếu không map được."},
    {"name": "SubTopicKey", "type": "BIGINT", "comment": "Thành phần của khóa tổ hợp định danh bản ghi sự kiện. Đồng thời là Khóa ngoại (FK) trỏ đến dim_sub_topic.SubTopicKey. Giá trị -1 nếu không map được hoặc không áp dụng."},
    {"name": "ArticleID_NK", "type": "STRING", "comment": "Thành phần của khóa tổ hợp định danh bản ghi sự kiện. Là ID gốc của bài viết từ `clean` (Degenerate Dimension)."},
    {"name": "ArticleTitle", "type": "STRING", "comment": "Tiêu đề bài viết. Degenerate Dimension."},
    {"name": "ArticleDescription", "type": "STRING", "comment": "Mô tả ngắn của bài viết. Degenerate Dimension."},
    {"name": "PublishedArticleCount", "type": "INT", "comment": "Measure: Số lượng bài viết được xuất bản (luôn là 1)."},
    {"name": "OpinionCount", "type": "INT", "comment": "Measure: Tổng số ý kiến/bình luận ban đầu (từ y_kien gốc)."},
    {"name": "WordCountInMainContent", "type": "INT", "comment": "Measure: Số từ trong nội dung chính."},
    {"name": "CharacterCountInMainContent", "type": "INT", "comment": "Measure: Số ký tự trong nội dung chính."},
    {"name": "EstimatedReadTimeMinutes", "type": "FLOAT", "comment": "Measure: Thời gian đọc ước tính (phút)."},
    {"name": "TaggedKeywordCountInArticle", "type": "INT", "comment": "Measure: Số lượng từ khóa riêng biệt được gắn cho bài viết."},
    {"name": "ReferenceSourceCountInArticle", "type": "INT", "comment": "Measure: Số lượng nguồn tham khảo riêng biệt được trích dẫn."}
]
fact_article_publication_table_comment = "Bảng sự kiện trung tâm ghi nhận các số liệu và thuộc tính cơ bản mỗi khi một bài viết được xuất bản."
create_iceberg_table_in_db_curated("fact_article_publication", fact_article_publication_fields, fact_article_publication_table_comment, CURATED_DATABASE_NAME, partitioned_by_sql="days(ArticlePublicationTimestamp)")

fact_article_keyword_fields = [
    {"name": "ArticlePublicationDateKey", "type": "INT", "comment": "Thành phần của khóa tổ hợp định danh bản ghi. Đồng thời là Khóa ngoại (FK) trỏ đến dim_date.DateKey (ngày xuất bản bài viết). Giá trị -1 nếu không map được."},
    {"name": "ArticleID_NK", "type": "STRING", "comment": "Thành phần của khóa tổ hợp định danh bản ghi. Đồng thời là Khóa ngoại (FK) đến ArticleID_NK của bài viết trong bảng fact_article_publication hoặc articles (clean)."}, # Giữ nguyên vì 'articles (clean)' không có nháy đơn bao quanh 'articles'
    {"name": "KeywordKey", "type": "BIGINT", "comment": "Thành phần của khóa tổ hợp định danh bản ghi. Đồng thời là Khóa ngoại (FK) trỏ đến dim_keyword.KeywordKey. Giá trị -1 nếu không map được."},
    {"name": "AuthorKey", "type": "BIGINT", "comment": "Khóa ngoại (FK) theo ngữ cảnh, trỏ đến dim_author.AuthorKey của tác giả bài viết. Giá trị -1 nếu không map được."},
    {"name": "TopicKey", "type": "BIGINT", "comment": "Khóa ngoại (FK) theo ngữ cảnh, trỏ đến dim_topic.TopicKey của chủ đề bài viết. Giá trị -1 nếu không map được."},
    {"name": "SubTopicKey", "type": "BIGINT", "comment": "Khóa ngoại (FK) theo ngữ cảnh, trỏ đến dim_sub_topic.SubTopicKey của chủ đề phụ bài viết. Giá trị -1 nếu không map được hoặc không áp dụng."},
    {"name": "IsKeywordTaggedToArticle", "type": "INT", "comment": "Measure: Cờ đánh dấu sự tồn tại của mối quan hệ (luôn là 1)."}
]
fact_article_keyword_table_comment = "Bảng sự kiện ghi nhận mối quan hệ giữa mỗi bài viết và từng từ khóa đã được gán (tag) cho nó."
create_iceberg_table_in_db_curated("fact_article_keyword", fact_article_keyword_fields, fact_article_keyword_table_comment, CURATED_DATABASE_NAME)

fact_article_reference_fields = [
    {"name": "ArticlePublicationDateKey", "type": "INT", "comment": "Thành phần của khóa tổ hợp định danh bản ghi. Đồng thời là Khóa ngoại (FK) trỏ đến dim_date.DateKey (ngày xuất bản bài viết). Giá trị -1 nếu không map được."},
    {"name": "ArticleID_NK", "type": "STRING", "comment": "Thành phần của khóa tổ hợp định danh bản ghi. Đồng thời là Khóa ngoại (FK) đến ArticleID_NK của bài viết."},
    {"name": "ReferenceSourceKey", "type": "BIGINT", "comment": "Thành phần của khóa tổ hợp định danh bản ghi. Đồng thời là Khóa ngoại (FK) trỏ đến dim_reference_source.ReferenceSourceKey. Giá trị -1 nếu không map được."},
    {"name": "AuthorKey", "type": "BIGINT", "comment": "Khóa ngoại (FK) theo ngữ cảnh, trỏ đến dim_author.AuthorKey. Giá trị -1 nếu không map được."},
    {"name": "TopicKey", "type": "BIGINT", "comment": "Khóa ngoại (FK) theo ngữ cảnh, trỏ đến dim_topic.TopicKey. Giá trị -1 nếu không map được."},
    {"name": "SubTopicKey", "type": "BIGINT", "comment": "Khóa ngoại (FK) theo ngữ cảnh, trỏ đến dim_sub_topic.SubTopicKey. Giá trị -1 nếu không map được hoặc không áp dụng."},
    {"name": "IsReferenceUsedInArticle", "type": "INT", "comment": "Measure: Cờ đánh dấu sự tồn tại của mối quan hệ (luôn là 1)."}
]
fact_article_reference_table_comment = "Bảng sự kiện ghi nhận mối quan hệ giữa mỗi bài viết và từng nguồn tham khảo được trích dẫn."
create_iceberg_table_in_db_curated("fact_article_reference", fact_article_reference_fields, fact_article_reference_table_comment, CURATED_DATABASE_NAME)

fact_top_comment_activity_fields = [
    {"name": "ArticlePublicationDateKey", "type": "INT", "comment": "Khóa ngoại (FK) trỏ đến dim_date.DateKey (ngày xuất bản bài viết). Giá trị -1 nếu không map được."},
    {"name": "CommentDateKey", "type": "INT", "comment": "Thành phần của khóa tổ hợp định danh bản ghi. Đồng thời là Khóa ngoại (FK) trỏ đến dim_date.DateKey (trong ETL này, nó bằng ArticlePublicationDateKey). Giá trị -1 nếu không map được."},
    {"name": "ArticleID_NK", "type": "STRING", "comment": "Thành phần của khóa tổ hợp định danh bản ghi. Đồng thời là Khóa ngoại (FK) đến ArticleID_NK của bài viết."},
    {"name": "CommentID_NK", "type": "STRING", "comment": "Thành phần của khóa tổ hợp định danh bản ghi. Là ID gốc của bình luận từ `clean`."},
    {"name": "AuthorKey", "type": "BIGINT", "comment": "Khóa ngoại (FK) theo ngữ cảnh, trỏ đến dim_author.AuthorKey. Giá trị -1 nếu không map được."},
    {"name": "TopicKey", "type": "BIGINT", "comment": "Khóa ngoại (FK) theo ngữ cảnh, trỏ đến dim_topic.TopicKey. Giá trị -1 nếu không map được."},
    {"name": "SubTopicKey", "type": "BIGINT", "comment": "Khóa ngoại (FK) theo ngữ cảnh, trỏ đến dim_sub_topic.SubTopicKey. Giá trị -1 nếu không map được hoặc không áp dụng."},
    {"name": "CommenterName", "type": "STRING", "comment": "Tên người bình luận. Degenerate Dimension."},
    {"name": "IsTopComment", "type": "INT", "comment": "Measure: Cờ đánh dấu đây là một bình luận `top` được ghi nhận (luôn là 1)."},
    {"name": "LikesOnTopComment", "type": "INT", "comment": "Measure: Tổng số lượt thích mà bình luận `top` này nhận được."}
]
fact_top_comment_activity_table_comment = "Bảng sự kiện ghi nhận các số liệu chính liên quan đến các bình luận `top` được crawl." # Sửa 'top' thành `top`
create_iceberg_table_in_db_curated("fact_top_comment_activity", fact_top_comment_activity_fields, fact_top_comment_activity_table_comment, CURATED_DATABASE_NAME)

fact_top_comment_interaction_detail_fields = [
    {"name": "ArticlePublicationDateKey", "type": "INT", "comment": "Khóa ngoại (FK) trỏ đến dim_date.DateKey (ngày xuất bản bài viết). Giá trị -1 nếu không map được."},
    {"name": "InteractionDateKey", "type": "INT", "comment": "Thành phần của khóa tổ hợp định danh bản ghi. Đồng thời là Khóa ngoại (FK) trỏ đến dim_date.DateKey (trong ETL này, nó bằng ArticlePublicationDateKey). Giá trị -1 nếu không map được."},
    {"name": "ArticleID_NK", "type": "STRING", "comment": "Thành phần của khóa tổ hợp định danh bản ghi. Đồng thời là Khóa ngoại (FK) đến ArticleID_NK của bài viết."},
    {"name": "CommentID_NK", "type": "STRING", "comment": "Thành phần của khóa tổ hợp định danh bản ghi. Đồng thời là Khóa ngoại (FK) đến CommentID_NK của bình luận."},
    {"name": "InteractionTypeKey", "type": "BIGINT", "comment": "Thành phần của khóa tổ hợp định danh bản ghi. Đồng thời là Khóa ngoại (FK) trỏ đến dim_interaction_type.InteractionTypeKey. Giá trị -1 nếu không map được."},
    {"name": "AuthorKey", "type": "BIGINT", "comment": "Khóa ngoại (FK) theo ngữ cảnh, trỏ đến dim_author.AuthorKey. Giá trị -1 nếu không map được."},
    {"name": "TopicKey", "type": "BIGINT", "comment": "Khóa ngoại (FK) theo ngữ cảnh, trỏ đến dim_topic.TopicKey. Giá trị -1 nếu không map được."},
    {"name": "SubTopicKey", "type": "BIGINT", "comment": "Khóa ngoại (FK) theo ngữ cảnh, trỏ đến dim_sub_topic.SubTopicKey. Giá trị -1 nếu không map được hoặc không áp dụng."},
    {"name": "InteractionInstanceCount", "type": "INT", "comment": "Measure: Số lần loại tương tác này xuất hiện trên bình luận (luôn là 1)."},
    {"name": "InteractionValue", "type": "INT", "comment": "Measure: Số lượt của loại tương tác đó (ví dụ: số lượt `Thích`)."}
]
fact_top_comment_interaction_detail_table_comment = "Bảng sự kiện ghi nhận chi tiết từng loại tương tác (Thích, Vui,...) trên các bình luận `top`." # Sửa 'top' thành `top`
create_iceberg_table_in_db_curated("fact_top_comment_interaction_detail", fact_top_comment_interaction_detail_fields, fact_top_comment_interaction_detail_table_comment, CURATED_DATABASE_NAME)

print(f"Hoàn tất việc tạo schema cho các bảng trong database '{CURATED_DATABASE_NAME}' của catalog '{curated_catalog_name}'.")
spark.stop()
