from pyspark.sql import SparkSession

# --- Cấu hình MinIO, Nessie (Không đổi) ---
minio_endpoint = "http://minio1:9000"
minio_access_key = "rH4arFLYBxl55rh2zmN1"
minio_secret_key = "AtEB2XiMAznxvJXRvaXxigdIewIMVKscgPg7dJjI"
nessie_uri = "http://nessie:19120/api/v2"
nessie_default_branch = "main"

# --- Cấu hình cho catalog và warehouse của vùng CLEAN ---
clean_catalog_name = "nessie-clean-news" # Giữ nguyên tên catalog Spark
clean_catalog_warehouse_path = "s3a://clean-news-lakehouse/nessie_clean_news_warehouse" # Giữ nguyên warehouse path
CLEAN_DATABASE_NAME = "news_clean_db" # ĐỊNH NGHĨA TÊN DATABASE/NAMESPACE TRONG NESSIE

app_name = "IcebergNessieCleanNews"

spark_builder = SparkSession.builder.appName(app_name)

# Cấu hình S3A
spark_builder = spark_builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Cấu hình Spark SQL Extensions
spark_builder = spark_builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")

# Cấu hình Catalog cho vùng CLEAN
spark_builder = spark_builder.config(f"spark.sql.catalog.{clean_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{clean_catalog_name}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config(f"spark.sql.catalog.{clean_catalog_name}.uri", nessie_uri) \
    .config(f"spark.sql.catalog.{clean_catalog_name}.ref", nessie_default_branch) \
    .config(f"spark.sql.catalog.{clean_catalog_name}.warehouse", clean_catalog_warehouse_path) \
    .config(f"spark.sql.catalog.{clean_catalog_name}.authentication.type", "NONE")
    # Nếu muốn đặt default namespace cho catalog này, bạn có thể thêm:
    # .config(f"spark.sql.catalog.{clean_catalog_name}.default-namespace", CLEAN_DATABASE_NAME)

# Đặt default catalog nếu muốn (tùy chọn, vì chúng ta sẽ dùng tên đầy đủ)
# spark_builder = spark_builder.config("spark.sql.defaultCatalog", clean_catalog_name)

spark = spark_builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("SparkSession đã được khởi tạo cho việc thiết lập vùng Clean (refactored).")
print(f"Cấu hình cho Spark Catalog '{clean_catalog_name}':")
print(f"  Implementation: {spark.conf.get(f'spark.sql.catalog.{clean_catalog_name}.catalog-impl')}")
print(f"  Nessie Tool URI: {spark.conf.get(f'spark.sql.catalog.{clean_catalog_name}.uri')}")
print(f"  Warehouse Path: {spark.conf.get(f'spark.sql.catalog.{clean_catalog_name}.warehouse')}")

# --- TẠO DATABASE/NAMESPACE TRONG NESSIE CHO VÙNG CLEAN ---
try:
    print(f"Đang tạo database/namespace '{CLEAN_DATABASE_NAME}' trong catalog '{clean_catalog_name}'...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{clean_catalog_name}`.`{CLEAN_DATABASE_NAME}`")
    print(f"Database/namespace `{clean_catalog_name}`.`{CLEAN_DATABASE_NAME}` đã được tạo (hoặc đã tồn tại).")
except Exception as e:
    print(f"Lỗi khi tạo database/namespace `{clean_catalog_name}`.`{CLEAN_DATABASE_NAME}`: {e}")

# Hàm trợ giúp để tạo bảng (tương tự như script curated)
def create_iceberg_table_in_db(table_name, schema_sql, db_name, catalog_name=clean_catalog_name, partitioned_by_sql=None):
    full_table_name_in_catalog = f"`{catalog_name}`.`{db_name}`.`{table_name}`"
    print(f"Đang tạo bảng {full_table_name_in_catalog}...")
    try:
        # spark.sql(f"DROP TABLE IF EXISTS {full_table_name_in_catalog}") # Cẩn thận!
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

# --- Định nghĩa schema và tạo bảng cho vùng CLEAN ---

authors_schema = """
    AuthorID STRING,
    AuthorName STRING
"""
create_iceberg_table_in_db("authors", authors_schema, CLEAN_DATABASE_NAME)

topics_schema = """
    TopicID STRING,
    TopicName STRING
"""
create_iceberg_table_in_db("topics", topics_schema, CLEAN_DATABASE_NAME)

subtopics_schema = """
    SubTopicID STRING,
    SubTopicName STRING,
    TopicID STRING
"""
create_iceberg_table_in_db("subtopics", subtopics_schema, CLEAN_DATABASE_NAME)

keywords_schema = """
    KeywordID STRING,
    KeywordText STRING
"""
create_iceberg_table_in_db("keywords", keywords_schema, CLEAN_DATABASE_NAME)

references_table_schema = """
    ReferenceID STRING,
    ReferenceText STRING
"""
create_iceberg_table_in_db("references_table", references_table_schema, CLEAN_DATABASE_NAME)

articles_schema = """
    ArticleID STRING,
    Title STRING,
    URL STRING,
    Description STRING,
    PublicationDate TIMESTAMP,
    MainContent STRING,
    OpinionCount INT,
    AuthorID STRING,
    TopicID STRING,
    SubTopicID STRING
"""
create_iceberg_table_in_db("articles", articles_schema, CLEAN_DATABASE_NAME, partitioned_by_sql="days(PublicationDate)")

article_keywords_schema = """
    ArticleID STRING,
    KeywordID STRING
"""
create_iceberg_table_in_db("article_keywords", article_keywords_schema, CLEAN_DATABASE_NAME)

article_references_schema = """
    ArticleID STRING,
    ReferenceID STRING
"""
create_iceberg_table_in_db("article_references", article_references_schema, CLEAN_DATABASE_NAME)

comments_schema = """
    CommentID STRING,
    ArticleID STRING,
    CommenterName STRING,
    CommentContent STRING,
    TotalLikes INT
"""
create_iceberg_table_in_db("comments", comments_schema, CLEAN_DATABASE_NAME)

comment_interactions_schema = """
    CommentInteractionID STRING,
    CommentID STRING,
    InteractionType STRING,
    InteractionCount INT
"""
create_iceberg_table_in_db("comment_interactions", comment_interactions_schema, CLEAN_DATABASE_NAME)

print(f"Hoàn tất việc tạo/cập nhật schema các bảng trong database '{CLEAN_DATABASE_NAME}' của catalog '{clean_catalog_name}'.")
spark.stop()
