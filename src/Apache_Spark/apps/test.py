from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, monotonically_increasing_id, to_date, year, month, dayofmonth,
    quarter, date_format, countDistinct, count, sum as _sum, lit,
    when, expr, coalesce, avg, round as _round, length, split, size
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType, FloatType, LongType, BooleanType
from pyspark.sql.window import Window

# --- Cấu hình MinIO và Nessie ---
minio_endpoint = "http://minio1:9000"
minio_access_key = "rH4arFLYBxl55rh2zmN1"
minio_secret_key = "AtEB2XiMAznxvJXRvaXxigdIewIMVKscgPg7dJjI"
nessie_uri = "http://nessie:19120/api/v2"
nessie_default_branch = "main"

# --- Cấu hình Catalog và Database cho CLEAN zone ---
clean_catalog_name = "nessie-clean-news"
clean_catalog_warehouse_path = "s3a://clean-news-lakehouse/nessie_clean_news_warehouse"
CLEAN_DATABASE_NAME = "news_clean_db"

# --- Cấu hình Catalog và Database cho CURATED zone ---
curated_catalog_name = "nessie-curated-news"
curated_catalog_warehouse_path = "s3a://curated-news-lakehouse/nessie_curated_news_warehouse"
CURATED_DATABASE_NAME = "news_curated_db"

app_name = "ETLCleanToCuratedNews"

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

# Cấu hình Catalog cho vùng CURATED
spark_builder = spark_builder.config(f"spark.sql.catalog.{curated_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{curated_catalog_name}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config(f"spark.sql.catalog.{curated_catalog_name}.uri", nessie_uri) \
    .config(f"spark.sql.catalog.{curated_catalog_name}.ref", nessie_default_branch) \
    .config(f"spark.sql.catalog.{curated_catalog_name}.warehouse", curated_catalog_warehouse_path) \
    .config(f"spark.sql.catalog.{curated_catalog_name}.authentication.type", "NONE")

spark = spark_builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("SparkSession đã được khởi tạo cho ETL từ Clean sang Curated.")
def read_clean_table(table_name, catalog=clean_catalog_name, db=CLEAN_DATABASE_NAME):
    full_table_name = f"`{catalog}`.`{db}`.`{table_name}`"
    print(f"Đang đọc từ bảng Clean: {full_table_name}")
    try:
        df = spark.read.format("iceberg").load(full_table_name)
        print(f"Đọc bảng {full_table_name} thành công. Số dòng: {df.count()}")
        return df
    except Exception as e:
        print(f"Lỗi khi đọc bảng {full_table_name} từ Clean: {e}")
        raise e
# Hàm trợ giúp để đọc bảng từ catalog và database cụ thể
def read_iceberg_table(catalog_name, db_name, table_name):
    full_table_name = f"`{catalog_name}`.`{db_name}`.`{table_name}`"
    print(f"Đang đọc từ bảng: {full_table_name}")
    try:
        df = spark.read.format("iceberg").load(full_table_name)
        print(f"Đọc bảng {full_table_name} thành công. Số dòng: {df.count()}")
        return df
    except Exception as e:
        print(f"Lỗi khi đọc bảng {full_table_name}: {e}")
        # spark.stop() # Cân nhắc việc dừng hẳn nếu lỗi nghiêm trọng
        # exit(1)
        return None # Trả về None để có thể kiểm tra và xử lý ở nơi gọi

# Hàm trợ giúp để ghi bảng vào catalog và database cụ thể
def write_curated_table(df_to_write, table_name, mode="overwrite", partition_cols=None):
    full_table_name = f"`{curated_catalog_name}`.`{CURATED_DATABASE_NAME}`.`{table_name}`"
    print(f"Đang ghi vào bảng Curated: {full_table_name} với mode '{mode}'")
    try:
        writer = df_to_write.writeTo(full_table_name) \
            .tableProperty("write.format.default", "parquet")

        if partition_cols:
            writer = writer.partitionedBy(*partition_cols)

        if mode == "overwrite_partitions" and partition_cols:
            writer.overwritePartitions()
        elif mode == "overwrite":
            writer.createOrReplace() # Dùng createOrReplace cho full load
        elif mode == "append":
            writer.append()
        else:
            raise ValueError(f"Chế độ ghi '{mode}' không được hỗ trợ.")

        print(f"Ghi vào bảng {full_table_name} thành công.")
    except Exception as e:
        print(f"Lỗi khi ghi vào bảng {full_table_name}: {e}")
        # spark.stop()
        # exit(1)

# --- 1. Đọc dữ liệu từ vùng CLEAN ---
print("\n--- Bước 1: Đọc dữ liệu từ vùng CLEAN ---")
authors_clean_df = read_iceberg_table(clean_catalog_name, CLEAN_DATABASE_NAME, "authors")
topics_clean_df = read_iceberg_table(clean_catalog_name, CLEAN_DATABASE_NAME, "topics")
subtopics_clean_df = read_iceberg_table(clean_catalog_name, CLEAN_DATABASE_NAME, "subtopics")
keywords_clean_df = read_iceberg_table(clean_catalog_name, CLEAN_DATABASE_NAME, "keywords")
references_clean_df = read_iceberg_table(clean_catalog_name, CLEAN_DATABASE_NAME, "references_table")
articles_clean_df = read_iceberg_table(clean_catalog_name, CLEAN_DATABASE_NAME, "articles")
article_keywords_clean_df = read_iceberg_table(clean_catalog_name, CLEAN_DATABASE_NAME, "article_keywords")
article_references_clean_df = read_iceberg_table(clean_catalog_name, CLEAN_DATABASE_NAME, "article_references")
comments_clean_df = read_iceberg_table(clean_catalog_name, CLEAN_DATABASE_NAME, "comments")
comment_interactions_clean_df = read_iceberg_table(clean_catalog_name, CLEAN_DATABASE_NAME, "comment_interactions")

# Kiểm tra nếu DataFrame nào đó không đọc được (là None)
if any(df is None for df in [authors_clean_df, topics_clean_df, subtopics_clean_df, keywords_clean_df, references_clean_df, articles_clean_df, article_keywords_clean_df, article_references_clean_df, comments_clean_df, comment_interactions_clean_df]):
    print("Một hoặc nhiều bảng từ vùng Clean không thể đọc được. Dừng ETL.")
    spark.stop()
    exit(1)

# --- 2. Xây dựng Bảng Chiều (Dimension Tables) cho CURATED ---
print("\n--- Bước 2: Xây dựng Dimension Tables cho CURATED ---")

# 2.1. DimDate
print("Đang xử lý DimDate...")
# Tạo DimDate dựa trên min/max PublicationDate từ articles_clean_df
# Đây là cách tạo DimDate đơn giản, trong thực tế bạn nên có script riêng
print("Đang đọc DimDate (đã được populate trước đó)...")
dim_date_df = read_clean_table("DimDate", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME) # Đọc từ curated
if dim_date_df is None or dim_date_df.count() == 0:
    print("LỖI NGHIÊM TRỌNG: Bảng DimDate rỗng hoặc không tồn tại. Dừng ETL.")
    spark.stop()
    exit(1)
dim_date_df.show(5, truncate=False)
# 2.2. DimAuthor
print("Đang xử lý DimAuthor...")
dim_author_df = authors_clean_df \
    .withColumn("AuthorKey", monotonically_increasing_id().cast(LongType())) \
    .withColumnRenamed("AuthorID", "AuthorID_NK") \
    .select("AuthorKey", "AuthorID_NK", "AuthorName")
dim_author_df.show(5)
write_curated_table(dim_author_df, "DimAuthor", mode="overwrite")

# 2.3. DimTopic
print("Đang xử lý DimTopic...")
dim_topic_df = topics_clean_df \
    .withColumn("TopicKey", monotonically_increasing_id().cast(LongType())) \
    .withColumnRenamed("TopicID", "TopicID_NK") \
    .select("TopicKey", "TopicID_NK", "TopicName")
dim_topic_df.show(5)
write_curated_table(dim_topic_df, "DimTopic", mode="overwrite")

# 2.4. DimSubTopic
print("Đang xử lý DimSubTopic...")
dim_subtopic_df = subtopics_clean_df \
    .join(dim_topic_df, subtopics_clean_df.TopicID == dim_topic_df.TopicID_NK, "left_outer") \
    .withColumn("SubTopicKey", monotonically_increasing_id().cast(LongType())) \
    .withColumnRenamed("SubTopicID", "SubTopicID_NK") \
    .withColumnRenamed("TopicName", "ParentTopicName") \
    .select("SubTopicKey", "SubTopicID_NK", "SubTopicName", col("TopicKey").alias("ParentTopicKey"), "ParentTopicName")
dim_subtopic_df.show(5)
write_curated_table(dim_subtopic_df, "DimSubTopic", mode="overwrite")

# 2.5. DimKeyword
print("Đang xử lý DimKeyword...")
dim_keyword_df = keywords_clean_df \
    .withColumn("KeywordKey", monotonically_increasing_id().cast(LongType())) \
    .withColumnRenamed("KeywordID", "KeywordID_NK") \
    .select("KeywordKey", "KeywordID_NK", "KeywordText")
dim_keyword_df.show(5)
write_curated_table(dim_keyword_df, "DimKeyword", mode="overwrite")

# 2.6. DimReferenceSource
print("Đang xử lý DimReferenceSource...")
dim_referencesource_df = references_clean_df \
    .withColumn("ReferenceSourceKey", monotonically_increasing_id().cast(LongType())) \
    .withColumnRenamed("ReferenceID", "ReferenceID_NK") \
    .select("ReferenceSourceKey", "ReferenceID_NK", "ReferenceText")
dim_referencesource_df.show(5)
write_curated_table(dim_referencesource_df, "DimReferenceSource", mode="overwrite")

# 2.7. DimInteractionType
print("Đang xử lý DimInteractionType...")
dim_interactiontype_df = comment_interactions_clean_df \
    .select(col("InteractionType").alias("InteractionTypeName")) \
    .filter(col("InteractionTypeName").isNotNull()) \
    .distinct() \
    .withColumn("InteractionTypeKey", monotonically_increasing_id().cast(LongType())) \
    .select("InteractionTypeKey", "InteractionTypeName")
dim_interactiontype_df.show(5)
write_curated_table(dim_interactiontype_df, "DimInteractionType", mode="overwrite")


# --- 3. Xây dựng Bảng Sự kiện (Fact Tables) cho CURATED ---
print("\n--- Bước 3: Xây dựng Fact Tables cho CURATED ---")

# 3.1. FactArticlePublication
print("Đang xử lý FactArticlePublication...")
# Tính toán các measures tổng hợp từ các bảng clean
# TaggedKeywordCountInArticle
tagged_keyword_counts = article_keywords_clean_df \
    .groupBy("ArticleID") \
    .agg(countDistinct("KeywordID").alias("TaggedKeywordCountInArticle"))

# ReferenceSourceCountInArticle
reference_source_counts = article_references_clean_df \
    .groupBy("ArticleID") \
    .agg(countDistinct("ReferenceID").alias("ReferenceSourceCountInArticle"))

# WordCount, CharCount, EstimatedReadTime
articles_with_text_metrics = articles_clean_df \
    .withColumn("WordCountInMainContent", when(col("MainContent").isNotNull(), size(split(col("MainContent"), " "))).otherwise(0)) \
    .withColumn("CharacterCountInMainContent", when(col("MainContent").isNotNull(), length(col("MainContent"))).otherwise(0)) \
    .withColumn("EstimatedReadTimeMinutes", _round(col("WordCountInMainContent") / 200.0, 2).cast(FloatType())) # Giả sử tốc độ đọc 200 từ/phút

# Nối tất cả lại
fact_article_publication_source = articles_with_text_metrics \
    .join(tagged_keyword_counts, articles_with_text_metrics.ArticleID == tagged_keyword_counts.ArticleID, "left_outer") \
    .join(reference_source_counts, articles_with_text_metrics.ArticleID == reference_source_counts.ArticleID, "left_outer") \
    .select(
        articles_with_text_metrics.ArticleID.alias("ArticleID_NK"),
        articles_with_text_metrics.Title.alias("ArticleTitle"),
        articles_with_text_metrics.Description.alias("ArticleDescription"),
        articles_with_text_metrics.PublicationDate.alias("ArticlePublicationTimestamp"),
        articles_with_text_metrics.OpinionCount,
        coalesce(articles_with_text_metrics.WordCountInMainContent, lit(0)).alias("WordCountInMainContent"),
        coalesce(articles_with_text_metrics.CharacterCountInMainContent, lit(0)).alias("CharacterCountInMainContent"),
        coalesce(articles_with_text_metrics.EstimatedReadTimeMinutes, lit(0.0)).alias("EstimatedReadTimeMinutes"),
        coalesce(tagged_keyword_counts.TaggedKeywordCountInArticle, lit(0)).alias("TaggedKeywordCountInArticle"),
        coalesce(reference_source_counts.ReferenceSourceCountInArticle, lit(0)).alias("ReferenceSourceCountInArticle"),
        articles_with_text_metrics.AuthorID.alias("AuthorID_NK_lookup"), # Để join lấy AuthorKey
        articles_with_text_metrics.TopicID.alias("TopicID_NK_lookup"),   # Để join lấy TopicKey
        articles_with_text_metrics.SubTopicID.alias("SubTopicID_NK_lookup") # Để join lấy SubTopicKey
    )

# Join với Dimensions để lấy Surrogate Keys
fact_article_publication_df = fact_article_publication_source \
    .withColumn("PublicationDateOnly", to_date(col("ArticlePublicationTimestamp"))) \
    .join(dim_date_df, col("PublicationDateOnly") == dim_date_df.FullDateAlternateKey, "left_outer") \
    .join(dim_author_df, col("AuthorID_NK_lookup") == dim_author_df.AuthorID_NK, "left_outer") \
    .join(dim_topic_df, col("TopicID_NK_lookup") == dim_topic_df.TopicID_NK, "left_outer") \
    .join(dim_subtopic_df, col("SubTopicID_NK_lookup") == dim_subtopic_df.SubTopicID_NK, "left_outer") \
    .select(
        col("DateKey").alias("PublicationDateKey"),
        col("ArticlePublicationTimestamp"),
        col("AuthorKey"),
        col("TopicKey"),
        col("SubTopicKey"),
        col("ArticleID_NK"),
        col("ArticleTitle"),
        col("ArticleDescription"),
        lit(1).alias("PublishedArticleCount"), # Measure
        col("OpinionCount"),                   # Measure
        col("WordCountInMainContent"),         # Measure
        col("CharacterCountInMainContent"),    # Measure
        col("EstimatedReadTimeMinutes"),       # Measure
        col("TaggedKeywordCountInArticle"),    # Measure
        col("ReferenceSourceCountInArticle")   # Measure
    )
# Đảm bảo các Key không null nếu chúng là PK logic
fact_article_publication_df = fact_article_publication_df.na.fill({
    "PublicationDateKey": -1, # Giá trị mặc định cho unknown date
    "AuthorKey": -1,          # Giá trị mặc định cho unknown author
    "TopicKey": -1,
    "SubTopicKey": -1
})

fact_article_publication_df.show(5, truncate=False)
write_curated_table(fact_article_publication_df, "FactArticlePublication", mode="overwrite", partition_cols=["ArticlePublicationTimestamp"]) # Phân vùng theo timestamp đầy đủ

# 3.2. FactArticleKeyword
print("Đang xử lý FactArticleKeyword...")
fact_article_keyword_df = article_keywords_clean_df \
    .join(articles_clean_df.select("ArticleID", "AuthorID", "TopicID", "SubTopicID", "PublicationDate"), ["ArticleID"], "inner") \
    .withColumn("PublicationDateOnly", to_date(col("PublicationDate"))) \
    .join(dim_date_df, col("PublicationDateOnly") == dim_date_df.FullDateAlternateKey, "left_outer") \
    .join(dim_keyword_df, article_keywords_clean_df.KeywordID == dim_keyword_df.KeywordID_NK, "left_outer") \
    .join(dim_author_df, col("AuthorID") == dim_author_df.AuthorID_NK, "left_outer") \
    .join(dim_topic_df, col("TopicID") == dim_topic_df.TopicID_NK, "left_outer") \
    .join(dim_subtopic_df, col("SubTopicID") == dim_subtopic_df.SubTopicID_NK, "left_outer") \
    .select(
        col("DateKey").alias("ArticlePublicationDateKey"),
        col("ArticleID").alias("ArticleID_NK"),
        col("KeywordKey"),
        col("AuthorKey"),
        col("TopicKey"),
        col("SubTopicKey"),
        lit(1).alias("IsKeywordTaggedToArticle") # Measure
    )
fact_article_keyword_df = fact_article_keyword_df.na.fill(-1, subset=["ArticlePublicationDateKey", "KeywordKey", "AuthorKey", "TopicKey", "SubTopicKey"])
fact_article_keyword_df.show(5)
write_curated_table(fact_article_keyword_df, "FactArticleKeyword", mode="overwrite")

# 3.3. FactArticleReference
print("Đang xử lý FactArticleReference...")
fact_article_reference_df = article_references_clean_df \
    .join(articles_clean_df.select("ArticleID", "AuthorID", "TopicID", "SubTopicID", "PublicationDate"), ["ArticleID"], "inner") \
    .withColumn("PublicationDateOnly", to_date(col("PublicationDate"))) \
    .join(dim_date_df, col("PublicationDateOnly") == dim_date_df.FullDateAlternateKey, "left_outer") \
    .join(dim_referencesource_df, article_references_clean_df.ReferenceID == dim_referencesource_df.ReferenceID_NK, "left_outer") \
    .join(dim_author_df, col("AuthorID") == dim_author_df.AuthorID_NK, "left_outer") \
    .join(dim_topic_df, col("TopicID") == dim_topic_df.TopicID_NK, "left_outer") \
    .join(dim_subtopic_df, col("SubTopicID") == dim_subtopic_df.SubTopicID_NK, "left_outer") \
    .select(
        col("DateKey").alias("ArticlePublicationDateKey"),
        col("ArticleID").alias("ArticleID_NK"),
        col("ReferenceSourceKey"),
        col("AuthorKey"),
        col("TopicKey"),
        col("SubTopicKey"),
        lit(1).alias("IsReferenceUsedInArticle") # Measure
    )
fact_article_reference_df = fact_article_reference_df.na.fill(-1, subset=["ArticlePublicationDateKey", "ReferenceSourceKey", "AuthorKey", "TopicKey", "SubTopicKey"])
fact_article_reference_df.show(5)
write_curated_table(fact_article_reference_df, "FactArticleReference", mode="overwrite")

# 3.4. FactTopCommentActivity
print("Đang xử lý FactTopCommentActivity...")
# Giả sử không có CommentDate riêng, dùng ArticlePublicationDateKey
fact_top_comment_activity_df = comments_clean_df \
    .join(articles_clean_df.select("ArticleID", "AuthorID", "TopicID", "SubTopicID", "PublicationDate"), ["ArticleID"], "inner") \
    .withColumn("PublicationDateOnly", to_date(col("PublicationDate"))) \
    .join(dim_date_df, col("PublicationDateOnly") == dim_date_df.FullDateAlternateKey, "left_outer") \
    .join(dim_author_df, col("AuthorID") == dim_author_df.AuthorID_NK, "left_outer") \
    .join(dim_topic_df, col("TopicID") == dim_topic_df.TopicID_NK, "left_outer") \
    .join(dim_subtopic_df, col("SubTopicID") == dim_subtopic_df.SubTopicID_NK, "left_outer") \
    .select(
        col("DateKey").alias("ArticlePublicationDateKey"),
        col("DateKey").alias("CommentDateKey"), # Sử dụng ngày xuất bản bài viết làm ngày bình luận (cần xem xét nếu có ngày bình luận thực)
        col("ArticleID").alias("ArticleID_NK"),
        col("CommentID").alias("CommentID_NK"),
        col("AuthorKey"),
        col("TopicKey"),
        col("SubTopicKey"),
        col("CommenterName"),
        lit(1).alias("IsTopComment"),                 # Measure
        col("TotalLikes").alias("LikesOnTopComment") # Measure
    )
fact_top_comment_activity_df = fact_top_comment_activity_df.na.fill({
    "ArticlePublicationDateKey": -1, "CommentDateKey": -1, "AuthorKey": -1, "TopicKey": -1, "SubTopicKey": -1
})
fact_top_comment_activity_df.show(5)
write_curated_table(fact_top_comment_activity_df, "FactTopCommentActivity", mode="overwrite")


# 3.5. FactTopCommentInteractionDetail
print("Đang xử lý FactTopCommentInteractionDetail...")
# Giả sử không có InteractionDate riêng, dùng CommentDateKey (tức ArticlePublicationDateKey)
fact_top_comment_interaction_detail_df = comment_interactions_clean_df \
    .join(comments_clean_df.select("CommentID", "ArticleID"), ["CommentID"], "inner") \
    .join(articles_clean_df.select("ArticleID", "AuthorID", "TopicID", "SubTopicID", "PublicationDate"), ["ArticleID"], "inner") \
    .withColumn("PublicationDateOnly", to_date(col("PublicationDate"))) \
    .join(dim_date_df, col("PublicationDateOnly") == dim_date_df.FullDateAlternateKey, "left_outer") \
    .join(dim_interactiontype_df, comment_interactions_clean_df.InteractionType == dim_interactiontype_df.InteractionTypeName, "left_outer") \
    .join(dim_author_df, col("AuthorID") == dim_author_df.AuthorID_NK, "left_outer") \
    .join(dim_topic_df, col("TopicID") == dim_topic_df.TopicID_NK, "left_outer") \
    .join(dim_subtopic_df, col("SubTopicID") == dim_subtopic_df.SubTopicID_NK, "left_outer") \
    .select(
        col("DateKey").alias("ArticlePublicationDateKey"),
        col("DateKey").alias("InteractionDateKey"), # Sử dụng ngày xuất bản bài viết làm ngày tương tác
        col("ArticleID").alias("ArticleID_NK"),
        col("CommentID").alias("CommentID_NK"),
        col("InteractionTypeKey"),
        col("AuthorKey"),
        col("TopicKey"),
        col("SubTopicKey"),
        lit(1).alias("InteractionInstanceCount"),      # Measure
        col("InteractionCount").alias("InteractionValue") # Measure
    )

fact_top_comment_interaction_detail_df = fact_top_comment_interaction_detail_df.na.fill({
    "ArticlePublicationDateKey": -1, "InteractionDateKey": -1, "InteractionTypeKey": -1,
    "AuthorKey": -1, "TopicKey": -1, "SubTopicKey": -1
})
fact_top_comment_interaction_detail_df.show(5)
write_curated_table(fact_top_comment_interaction_detail_df, "FactTopCommentInteractionDetail", mode="overwrite")

print("\n--- Hoàn thành quy trình ETL từ Clean sang Curated ---")
spark.stop()
