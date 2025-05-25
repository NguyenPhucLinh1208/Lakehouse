from pyspark.sql import SparkSession

# --- Cấu hình cho MinIO và Nessie (Tương tự như clean) ---
minio_endpoint = "http://minio1:9000"
minio_access_key = "rH4arFLYBxl55rh2zmN1"
minio_secret_key = "AtEB2XiMAznxvJXRvaXxigdIewIMVKscgPg7dJjI"
nessie_uri = "http://nessie:19120/api/v2"
nessie_default_branch = "main" # Hoặc bạn có thể dùng nhánh khác cho curated

# --- Cấu hình MỚI cho catalog, warehouse và DATABASE của vùng CURATED ---
curated_catalog_name = "nessie-curated-news"
curated_catalog_warehouse_path = "s3a://curated-news-lakehouse/nessie_curated_news_warehouse"
CURATED_DATABASE_NAME = "news_curated_db" # ĐỊNH NGHĨA TÊN DATABASE/NAMESPACE TRONG NESSIE

app_name = "IcebergNessieCuratedNewsSetupRefactored"

spark_builder = SparkSession.builder.appName(app_name)

# Cấu hình S3A (giống như clean)
spark_builder = spark_builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Cấu hình Spark SQL Extensions (giống như clean)
spark_builder = spark_builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")

# --- Cấu hình Catalog cho vùng CURATED ---
spark_builder = spark_builder.config(f"spark.sql.catalog.{curated_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{curated_catalog_name}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config(f"spark.sql.catalog.{curated_catalog_name}.uri", nessie_uri) \
    .config(f"spark.sql.catalog.{curated_catalog_name}.ref", nessie_default_branch) \
    .config(f"spark.sql.catalog.{curated_catalog_name}.warehouse", curated_catalog_warehouse_path) \
    .config(f"spark.sql.catalog.{curated_catalog_name}.authentication.type", "NONE")
    # Nếu muốn đặt default namespace cho catalog này, bạn có thể thêm:
    # .config(f"spark.sql.catalog.{curated_catalog_name}.default-namespace", CURATED_DATABASE_NAME)

# Bạn có thể đặt default catalog là curated nếu script này chỉ làm việc với curated
# spark_builder = spark_builder.config("spark.sql.defaultCatalog", curated_catalog_name)

spark = spark_builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("SparkSession đã được khởi tạo cho việc thiết lập vùng Curated (refactored).")
print(f"Cấu hình cho Spark Catalog '{curated_catalog_name}':")
print(f"  Implementation: {spark.conf.get(f'spark.sql.catalog.{curated_catalog_name}.catalog-impl')}")
print(f"  Nessie Tool URI: {spark.conf.get(f'spark.sql.catalog.{curated_catalog_name}.uri')}")
print(f"  Warehouse Path: {spark.conf.get(f'spark.sql.catalog.{curated_catalog_name}.warehouse')}")

# --- TẠO DATABASE/NAMESPACE TRONG NESSIE CHO VÙNG CURATED ---
try:
    print(f"Đang tạo database/namespace '{CURATED_DATABASE_NAME}' trong catalog '{curated_catalog_name}'...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{curated_catalog_name}`.`{CURATED_DATABASE_NAME}`")
    print(f"Database/namespace `{curated_catalog_name}`.`{CURATED_DATABASE_NAME}` đã được tạo (hoặc đã tồn tại).")
except Exception as e:
    print(f"Lỗi khi tạo database/namespace `{curated_catalog_name}`.`{CURATED_DATABASE_NAME}`: {e}")

# Hàm trợ giúp để tạo bảng (tương tự như script clean refactored)
def create_iceberg_table_in_db_curated(table_name, schema_sql, db_name, catalog_name=curated_catalog_name, partitioned_by_sql=None):
    full_table_name_in_catalog = f"`{catalog_name}`.`{db_name}`.`{table_name}`"
    print(f"Đang tạo bảng {full_table_name_in_catalog}...")
    try:
        # spark.sql(f"DROP TABLE IF EXISTS {full_table_name_in_catalog}") # Cẩn thận khi dùng drop!
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name_in_catalog} (
            {schema_sql}
        ) USING iceberg
        """
        if partitioned_by_sql:
            create_sql += f"\nPARTITIONED BY ({partitioned_by_sql})"
        create_sql += ";"

        spark.sql(create_sql)
        print(f"Bảng {full_table_name_in_catalog} đã được tạo (hoặc đã tồn tại).")
        spark.sql(f"DESCRIBE TABLE EXTENDED {full_table_name_in_catalog}").show(truncate=False, n=100)
    except Exception as e:
        print(f"Lỗi khi tạo bảng {full_table_name_in_catalog}: {e}")

# --- I. Bảng Chiều (Dimension Tables) ---

dim_date_schema = """
    DateKey INT,
    FullDateAlternateKey DATE,
    DayNameOfWeek STRING,
    DayNumberOfMonth INT,
    DayNumberOfYear INT,
    MonthName STRING,
    MonthNumberOfYear INT,
    CalendarQuarter INT,
    CalendarYear INT
"""
create_iceberg_table_in_db_curated("DimDate", dim_date_schema, CURATED_DATABASE_NAME)

dim_author_schema = """
    AuthorKey BIGINT,
    AuthorID_NK STRING,
    AuthorName STRING
"""
create_iceberg_table_in_db_curated("DimAuthor", dim_author_schema, CURATED_DATABASE_NAME)

dim_topic_schema = """
    TopicKey BIGINT,
    TopicID_NK STRING,
    TopicName STRING
"""
create_iceberg_table_in_db_curated("DimTopic", dim_topic_schema, CURATED_DATABASE_NAME)

dim_subtopic_schema = """
    SubTopicKey BIGINT,
    SubTopicID_NK STRING,
    SubTopicName STRING,
    ParentTopicKey BIGINT,
    ParentTopicName STRING
"""
create_iceberg_table_in_db_curated("DimSubTopic", dim_subtopic_schema, CURATED_DATABASE_NAME)

dim_keyword_schema = """
    KeywordKey BIGINT,
    KeywordID_NK STRING,
    KeywordText STRING
"""
create_iceberg_table_in_db_curated("DimKeyword", dim_keyword_schema, CURATED_DATABASE_NAME)

dim_referencesource_schema = """
    ReferenceSourceKey BIGINT,
    ReferenceID_NK STRING,
    ReferenceText STRING
"""
create_iceberg_table_in_db_curated("DimReferenceSource", dim_referencesource_schema, CURATED_DATABASE_NAME)

dim_interactiontype_schema = """
    InteractionTypeKey BIGINT,
    InteractionTypeName STRING
"""
create_iceberg_table_in_db_curated("DimInteractionType", dim_interactiontype_schema, CURATED_DATABASE_NAME)

# --- II. Bảng Sự kiện (Fact Tables) ---

fact_article_publication_schema = """
    PublicationDateKey INT,
    ArticlePublicationTimestamp TIMESTAMP,
    AuthorKey BIGINT,
    TopicKey BIGINT,
    SubTopicKey BIGINT,
    ArticleID_NK STRING,
    ArticleTitle STRING,
    ArticleDescription STRING,
    PublishedArticleCount INT,
    OpinionCount INT,
    WordCountInMainContent INT,
    CharacterCountInMainContent INT,
    EstimatedReadTimeMinutes FLOAT,
    TaggedKeywordCountInArticle INT,
    ReferenceSourceCountInArticle INT
"""
create_iceberg_table_in_db_curated("FactArticlePublication", fact_article_publication_schema, CURATED_DATABASE_NAME, partitioned_by_sql="days(ArticlePublicationTimestamp)")

fact_article_keyword_schema = """
    ArticlePublicationDateKey INT,
    ArticleID_NK STRING,
    KeywordKey BIGINT,
    AuthorKey BIGINT,
    TopicKey BIGINT,
    SubTopicKey BIGINT,
    IsKeywordTaggedToArticle INT
"""
create_iceberg_table_in_db_curated("FactArticleKeyword", fact_article_keyword_schema, CURATED_DATABASE_NAME)

fact_article_reference_schema = """
    ArticlePublicationDateKey INT,
    ArticleID_NK STRING,
    ReferenceSourceKey BIGINT,
    AuthorKey BIGINT,
    TopicKey BIGINT,
    SubTopicKey BIGINT,
    IsReferenceUsedInArticle INT
"""
create_iceberg_table_in_db_curated("FactArticleReference", fact_article_reference_schema, CURATED_DATABASE_NAME)

fact_top_comment_activity_schema = """
    ArticlePublicationDateKey INT,
    CommentDateKey INT,
    ArticleID_NK STRING,
    CommentID_NK STRING,
    AuthorKey BIGINT,
    TopicKey BIGINT,
    SubTopicKey BIGINT,
    CommenterName STRING,
    IsTopComment INT,
    LikesOnTopComment INT
"""
create_iceberg_table_in_db_curated("FactTopCommentActivity", fact_top_comment_activity_schema, CURATED_DATABASE_NAME)

fact_top_comment_interaction_detail_schema = """
    ArticlePublicationDateKey INT,
    InteractionDateKey INT,
    ArticleID_NK STRING,
    CommentID_NK STRING,
    InteractionTypeKey BIGINT,
    AuthorKey BIGINT,
    TopicKey BIGINT,
    SubTopicKey BIGINT,
    InteractionInstanceCount INT,
    InteractionValue INT
"""
create_iceberg_table_in_db_curated("FactTopCommentInteractionDetail", fact_top_comment_interaction_detail_schema, CURATED_DATABASE_NAME)

print(f"Hoàn tất việc tạo schema cho các bảng trong database '{CURATED_DATABASE_NAME}' của catalog '{curated_catalog_name}'.")
spark.stop()
