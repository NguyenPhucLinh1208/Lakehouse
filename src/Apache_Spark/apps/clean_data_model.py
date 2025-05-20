from pyspark.sql import SparkSession

minio_endpoint = "http://minio1:9000"
minio_access_key = "rH4arFLYBxl55rh2zmN1"
minio_secret_key = "AtEB2XiMAznxvJXRvaXxigdIewIMVKscgPg7dJjI"
nessie_uri = "http://nessie:19120/api/v2"
nessie_default_branch = "main"
catalog_warehouse_path = "s3a://clean-news-lakehouse/nessie_clean_news_warehouse"

app_name = "IcebergNessieCleanNewsSetup"

spark_builder = SparkSession.builder.appName(app_name)

spark_builder = spark_builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark_builder = spark_builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")

spark_builder = spark_builder.config("spark.sql.catalog.nessie-clean-news", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie-clean-news.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie-clean-news.uri", nessie_uri) \
    .config("spark.sql.catalog.nessie-clean-news.ref", nessie_default_branch) \
    .config("spark.sql.catalog.nessie-clean-news.warehouse", catalog_warehouse_path) \
    .config("spark.sql.catalog.nessie-clean-news.authentication.type", "NONE")

spark_builder = spark_builder.config("spark.sql.defaultCatalog", "nessie-clean-news")

spark = spark_builder.getOrCreate()

print("SparkSession đã được khởi tạo.")
print(f"Cấu hình cho Spark Catalog 'nessie-clean-news':")
print(f"  Implementation: {spark.conf.get('spark.sql.catalog.nessie-clean-news.catalog-impl')}")
print(f"  Nessie Tool URI: {spark.conf.get('spark.sql.catalog.nessie-clean-news.uri')}")
print(f"  Warehouse Path: {spark.conf.get('spark.sql.catalog.nessie-clean-news.warehouse')}")
print(f"Default Spark Catalog: {spark.conf.get('spark.sql.defaultCatalog')}")

print("Đang tạo bảng AUTHORS...")
spark.sql("""
CREATE TABLE IF NOT EXISTS authors (
    AuthorID INT COMMENT 'Khóa chính, ID duy nhất cho mỗi tác giả',
    AuthorName STRING COMMENT 'Tên của tác giả'
) USING iceberg
COMMENT 'Bảng lưu trữ thông tin về tác giả bài viết';
""")
print("Bảng AUTHORS đã được tạo (hoặc đã tồn tại).")
spark.sql("DESCRIBE TABLE EXTENDED authors").show(truncate=False, n=100)

print("Đang tạo bảng TOPICS...")
spark.sql("""
CREATE TABLE IF NOT EXISTS topics (
    TopicID INT COMMENT 'Khóa chính, ID duy nhất cho mỗi chủ đề',
    TopicName STRING COMMENT 'Tên của chủ đề'
) USING iceberg
COMMENT 'Bảng lưu trữ các chủ đề chính của tin tức';
""")
print("Bảng TOPICS đã được tạo (hoặc đã tồn tại).")
spark.sql("DESCRIBE TABLE EXTENDED topics").show(truncate=False, n=100)

print("Đang tạo bảng SUBTOPICS...")
spark.sql("""
CREATE TABLE IF NOT EXISTS subtopics (
    SubTopicID INT COMMENT 'Khóa chính, ID duy nhất cho mỗi chủ đề phụ',
    SubTopicName STRING COMMENT 'Tên của chủ đề phụ',
    TopicID INT COMMENT 'Khóa ngoại, tham chiếu đến bảng TOPICS (TopicID)'
) USING iceberg
COMMENT 'Bảng lưu trữ các chủ đề phụ, liên kết với chủ đề chính';
""")
print("Bảng SUBTOPICS đã được tạo (hoặc đã tồn tại).")
spark.sql("DESCRIBE TABLE EXTENDED subtopics").show(truncate=False, n=100)

print("Đang tạo bảng KEYWORDS...")
spark.sql("""
CREATE TABLE IF NOT EXISTS keywords (
    KeywordID INT COMMENT 'Khóa chính, ID duy nhất cho mỗi từ khóa',
    KeywordText STRING COMMENT 'Nội dung của từ khóa'
) USING iceberg
COMMENT 'Bảng lưu trữ các từ khóa được sử dụng trong bài viết';
""")
print("Bảng KEYWORDS đã được tạo (hoặc đã tồn tại).")
spark.sql("DESCRIBE TABLE EXTENDED keywords").show(truncate=False, n=100)

print("Đang tạo bảng REFERENCES_TABLE...")
spark.sql("""
CREATE TABLE IF NOT EXISTS references_table (
    ReferenceID INT COMMENT 'Khóa chính, ID duy nhất cho mỗi nguồn tham khảo',
    ReferenceText STRING COMMENT 'Nội dung/tên của nguồn tham khảo'
) USING iceberg
COMMENT 'Bảng lưu trữ các nguồn tham khảo của bài viết';
""")
print("Bảng REFERENCES_TABLE đã được tạo (hoặc đã tồn tại).")
spark.sql("DESCRIBE TABLE EXTENDED references_table").show(truncate=False, n=100)

print("Đang tạo bảng ARTICLES...")
spark.sql("""
CREATE TABLE IF NOT EXISTS articles (
    ArticleID INT COMMENT 'Khóa chính, ID duy nhất cho mỗi bài viết',
    Title STRING COMMENT 'Tiêu đề của bài viết',
    URL STRING COMMENT 'Đường dẫn URL gốc của bài viết',
    Description STRING COMMENT 'Mô tả ngắn của bài viết',
    PublicationDate TIMESTAMP COMMENT 'Ngày và giờ xuất bản bài viết',
    MainContent STRING COMMENT 'Nội dung chính của bài viết',
    OpinionCount INT COMMENT 'Số lượng ý kiến/bình luận',
    AuthorID INT COMMENT 'Khóa ngoại, tham chiếu đến bảng AUTHORS (AuthorID)',
    TopicID INT COMMENT 'Khóa ngoại, tham chiếu đến bảng TOPICS (TopicID)',
    SubTopicID INT COMMENT 'Khóa ngoại, tham chiếu đến bảng SUBTOPICS (SubTopicID)'
) USING iceberg
COMMENT 'Bảng chính lưu trữ thông tin chi tiết của các bài viết'
PARTITIONED BY (days(PublicationDate)); 
""")
# PARTITIONED BY (days(PublicationDate)) sẽ tạo phân vùng dựa trên ngày của cột PublicationDate.
# Điều này rất tốt cho việc truy vấn theo khoảng thời gian.
print("Bảng ARTICLES đã được tạo (hoặc đã tồn tại).")
spark.sql("DESCRIBE TABLE EXTENDED articles").show(truncate=False, n=100)

print("Đang tạo bảng ARTICLEKEYWORDS...")
spark.sql("""
CREATE TABLE IF NOT EXISTS article_keywords (
    ArticleID INT COMMENT 'Khóa ngoại, tham chiếu đến bảng ARTICLES (ArticleID)',
    KeywordID INT COMMENT 'Khóa ngoại, tham chiếu đến bảng KEYWORDS (KeywordID)'
) USING iceberg
COMMENT 'Bảng nối giữa bài viết và từ khóa (quan hệ nhiều-nhiều)';
""")
print("Bảng ARTICLEKEYWORDS đã được tạo (hoặc đã tồn tại).")
spark.sql("DESCRIBE TABLE EXTENDED article_keywords").show(truncate=False, n=100)

print("Đang tạo bảng ARTICLEREFERENCES...")
spark.sql("""
CREATE TABLE IF NOT EXISTS article_references (
    ArticleID INT COMMENT 'Khóa ngoại, tham chiếu đến bảng ARTICLES (ArticleID)',
    ReferenceID INT COMMENT 'Khóa ngoại, tham chiếu đến bảng REFERENCES_TABLE (ReferenceID)'
) USING iceberg
COMMENT 'Bảng nối giữa bài viết và nguồn tham khảo (quan hệ nhiều-nhiều)';
""")
print("Bảng ARTICLEREFERENCES đã được tạo (hoặc đã tồn tại).")
spark.sql("DESCRIBE TABLE EXTENDED article_references").show(truncate=False, n=100)

print("Đang tạo bảng COMMENTS...")
spark.sql("""
CREATE TABLE IF NOT EXISTS comments (
    CommentID INT COMMENT 'Khóa chính, ID duy nhất cho mỗi bình luận',
    ArticleID INT COMMENT 'Khóa ngoại, tham chiếu đến bảng ARTICLES (ArticleID) mà bình luận này thuộc về',
    CommenterName STRING COMMENT 'Tên người bình luận',
    CommentContent STRING COMMENT 'Nội dung bình luận',
    TotalLikes INT COMMENT 'Tổng số lượt thích cho bình luận'
) USING iceberg
COMMENT 'Bảng lưu trữ các bình luận của bài viết';
""")
print("Bảng COMMENTS đã được tạo (hoặc đã tồn tại).")
spark.sql("DESCRIBE TABLE EXTENDED comments").show(truncate=False, n=100)

print("Đang tạo bảng COMMENTINTERACTIONS...")
spark.sql("""
CREATE TABLE IF NOT EXISTS comment_interactions (
    CommentInteractionID INT COMMENT 'Khóa chính, ID duy nhất cho mỗi tương tác bình luận',
    CommentID INT COMMENT 'Khóa ngoại, tham chiếu đến bảng COMMENTS (CommentID)',
    InteractionType STRING COMMENT 'Loại tương tác (ví dụ: Thích, Vui, Buồn)',
    InteractionCount INT COMMENT 'Số lượng của loại tương tác này'
) USING iceberg
COMMENT 'Bảng lưu trữ chi tiết các loại tương tác cho mỗi bình luận';
""")
print("Bảng COMMENTINTERACTIONS đã được tạo (hoặc đã tồn tại).")
spark.sql("DESCRIBE TABLE EXTENDED comment_interactions").show(truncate=False, n=100)


spark.stop()
