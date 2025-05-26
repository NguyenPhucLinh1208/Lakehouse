from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, monotonically_increasing_id, to_date, year, month, dayofmonth,
    quarter, date_format, countDistinct, count, sum as _sum, lit,
    when, expr, coalesce, avg, round as _round, length, split, size,
    row_number
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

app_name = "InitialLoadCleanToCuratedNewsETL"

spark_builder = SparkSession.builder.appName(app_name)

# --- Cấu hình SparkSession (S3A, Extensions, Catalogs) ---
spark_builder = spark_builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark_builder = spark_builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
# Clean Catalog
spark_builder = spark_builder.config(f"spark.sql.catalog.{clean_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{clean_catalog_name}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config(f"spark.sql.catalog.{clean_catalog_name}.uri", nessie_uri) \
    .config(f"spark.sql.catalog.{clean_catalog_name}.ref", nessie_default_branch) \
    .config(f"spark.sql.catalog.{clean_catalog_name}.warehouse", clean_catalog_warehouse_path) \
    .config(f"spark.sql.catalog.{clean_catalog_name}.authentication.type", "NONE")
# Curated Catalog
spark_builder = spark_builder.config(f"spark.sql.catalog.{curated_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{curated_catalog_name}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config(f"spark.sql.catalog.{curated_catalog_name}.uri", nessie_uri) \
    .config(f"spark.sql.catalog.{curated_catalog_name}.ref", nessie_default_branch) \
    .config(f"spark.sql.catalog.{curated_catalog_name}.warehouse", curated_catalog_warehouse_path) \
    .config(f"spark.sql.catalog.{curated_catalog_name}.authentication.type", "NONE")

spark = spark_builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("SparkSession đã được khởi tạo cho Initial Load: Clean -> Curated.")

def read_clean_table(table_name, catalog=clean_catalog_name, db=CLEAN_DATABASE_NAME):
    full_table_name = f"`{catalog}`.`{db}`.`{table_name}`"
    print(f"Đang đọc từ bảng: {full_table_name}") # Sửa "Clean" thành chung chung hơn
    try:
        df = spark.read.format("iceberg").load(full_table_name)
        print(f"Đọc bảng {full_table_name} thành công. Số dòng: {df.count()}")
        return df
    except Exception as e:
        print(f"Lỗi khi đọc bảng {full_table_name}: {e}") # Sửa "Clean" thành chung chung hơn
        raise e

def write_curated_table_initial_load(df_to_write, table_name, partition_cols=None, mode="overwrite"):
    full_table_name = f"`{curated_catalog_name}`.`{CURATED_DATABASE_NAME}`.`{table_name}`" # table_name đã là snake_case
    print(f"Đang ghi (Mode: {mode}) vào bảng Curated: {full_table_name}")
    try:
        writer = df_to_write.writeTo(full_table_name) \
            .tableProperty("write.format.default", "parquet")

        if partition_cols:
            writer = writer.partitionedBy(*partition_cols)

        if mode.lower() == "overwrite":
            writer.createOrReplace()
        elif mode.lower() == "append":
            writer.append()
        else:
            raise ValueError(f"Chế độ ghi '{mode}' không được hỗ trợ. Chỉ 'overwrite' hoặc 'append'.")

        print(f"Ghi (Mode: {mode}) vào bảng {full_table_name} thành công.")
    except Exception as e:
        print(f"Lỗi khi ghi (Mode: {mode}) vào bảng {full_table_name}: {e}")
        raise e

# --- 1. Đọc dữ liệu từ vùng CLEAN (toàn bộ lịch sử) ---
print("\n--- Bước 1: Đọc dữ liệu từ vùng CLEAN ---")
authors_clean_df = read_clean_table("authors")
topics_clean_df = read_clean_table("topics")
subtopics_clean_df = read_clean_table("subtopics")
keywords_clean_df = read_clean_table("keywords")
references_clean_df = read_clean_table("references_table")
articles_clean_df = read_clean_table("articles")
article_keywords_clean_df = read_clean_table("article_keywords")
article_references_clean_df = read_clean_table("article_references")
comments_clean_df = read_clean_table("comments")
comment_interactions_clean_df = read_clean_table("comment_interactions")

# --- 2. Xây dựng Bảng Chiều (Dimension Tables) cho CURATED ---
print("\n--- Bước 2: Xây dựng Dimension Tables cho CURATED (Initial Load) ---")

# 2.1. DimDate
print("Đang đọc dim_date (đã được populate trước đó)...")
dim_date_df = read_clean_table("dim_date", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME)
if dim_date_df is None or dim_date_df.count() == 0:
    print("LỖI NGHIÊM TRỌNG: Bảng dim_date rỗng hoặc không tồn tại. Dừng ETL.")
    spark.stop()
    exit(1)
dim_date_df.show(5, truncate=False)


# 2.2. DimAuthor
print("Đang xử lý dim_author...")
dim_author_df = authors_clean_df \
    .withColumn("AuthorKey", monotonically_increasing_id().cast(LongType())) \
    .select(col("AuthorKey"), col("AuthorID").alias("AuthorID_NK"), col("AuthorName"))
write_curated_table_initial_load(dim_author_df, "dim_author")

# 2.3. DimTopic
print("Đang xử lý dim_topic...")
dim_topic_df = topics_clean_df \
    .withColumn("TopicKey", monotonically_increasing_id().cast(LongType())) \
    .select(col("TopicKey"), col("TopicID").alias("TopicID_NK"), col("TopicName"))
write_curated_table_initial_load(dim_topic_df, "dim_topic")

# 2.4. DimSubTopic
print("Đang xử lý dim_sub_topic...")
dim_topic_curated_df = read_clean_table("dim_topic", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME)

dim_subtopic_df = subtopics_clean_df \
    .join(dim_topic_curated_df, subtopics_clean_df.TopicID == dim_topic_curated_df.TopicID_NK, "left_outer") \
    .withColumn("SubTopicKey", monotonically_increasing_id().cast(LongType())) \
    .select(
        col("SubTopicKey"),
        col("SubTopicID").alias("SubTopicID_NK"),
        col("SubTopicName"),
        col("TopicKey").alias("ParentTopicKey"),
        col("TopicName").alias("ParentTopicName")
    )
write_curated_table_initial_load(dim_subtopic_df, "dim_sub_topic")

# 2.5. DimKeyword
print("Đang xử lý dim_keyword...")
dim_keyword_df = keywords_clean_df \
    .withColumn("KeywordKey", monotonically_increasing_id().cast(LongType())) \
    .select(col("KeywordKey"), col("KeywordID").alias("KeywordID_NK"), col("KeywordText"))
write_curated_table_initial_load(dim_keyword_df, "dim_keyword")

# 2.6. DimReferenceSource
print("Đang xử lý dim_reference_source...")
dim_referencesource_df = references_clean_df \
    .withColumn("ReferenceSourceKey", monotonically_increasing_id().cast(LongType())) \
    .select(col("ReferenceSourceKey"), col("ReferenceID").alias("ReferenceID_NK"), col("ReferenceText"))
write_curated_table_initial_load(dim_referencesource_df, "dim_reference_source")

# 2.7. DimInteractionType
print("Đang xử lý dim_interaction_type...")
dim_interactiontype_df = comment_interactions_clean_df \
    .select(col("InteractionType").alias("InteractionTypeName")) \
    .filter(col("InteractionTypeName").isNotNull()) \
    .distinct() \
    .withColumn("InteractionTypeKey", monotonically_increasing_id().cast(LongType())) \
    .select("InteractionTypeKey", "InteractionTypeName")
write_curated_table_initial_load(dim_interactiontype_df, "dim_interaction_type")

# --- 3. Xây dựng Bảng Sự kiện (Fact Tables) cho CURATED (Initial Load) ---
print("\n--- Bước 3: Xây dựng Fact Tables cho CURATED (Initial Load) ---")
print("Đọc lại các Dimension từ Curated để lấy Surrogate Keys...")
dim_date_loaded_df = read_clean_table("dim_date", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME)
dim_author_loaded_df = read_clean_table("dim_author", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME)
dim_topic_loaded_df = read_clean_table("dim_topic", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME)
dim_subtopic_loaded_df = read_clean_table("dim_sub_topic", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME)
dim_keyword_loaded_df = read_clean_table("dim_keyword", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME)
dim_referencesource_loaded_df = read_clean_table("dim_reference_source", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME)
dim_interactiontype_loaded_df = read_clean_table("dim_interaction_type", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME)


# 3.1. FactArticlePublication
print("Đang xử lý fact_article_publication...")
tagged_keyword_counts = article_keywords_clean_df \
    .groupBy("ArticleID") \
    .agg(countDistinct("KeywordID").alias("TaggedKeywordCountInArticle"))

reference_source_counts = article_references_clean_df \
    .groupBy("ArticleID") \
    .agg(countDistinct("ReferenceID").alias("ReferenceSourceCountInArticle"))

articles_with_text_metrics = articles_clean_df \
    .withColumn("WordCountInMainContent", when(col("MainContent").isNotNull(), size(split(col("MainContent"), " "))).otherwise(0)) \
    .withColumn("CharacterCountInMainContent", when(col("MainContent").isNotNull(), length(col("MainContent"))).otherwise(0)) \
    .withColumn("EstimatedReadTimeMinutes", coalesce(_round(col("WordCountInMainContent") / 200.0, 2).cast(FloatType()), lit(0.0)))

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
        col("EstimatedReadTimeMinutes"),
        coalesce(tagged_keyword_counts.TaggedKeywordCountInArticle, lit(0)).alias("TaggedKeywordCountInArticle"),
        coalesce(reference_source_counts.ReferenceSourceCountInArticle, lit(0)).alias("ReferenceSourceCountInArticle"),
        articles_with_text_metrics.AuthorID.alias("AuthorID_lookup"),
        articles_with_text_metrics.TopicID.alias("TopicID_lookup"),
        articles_with_text_metrics.SubTopicID.alias("SubTopicID_lookup")
    )

fact_article_publication_df = fact_article_publication_source \
    .withColumn("PublicationDateOnly", to_date(col("ArticlePublicationTimestamp"))) \
    .join(dim_date_loaded_df, col("PublicationDateOnly") == dim_date_loaded_df.FullDateAlternateKey, "left_outer") \
    .join(dim_author_loaded_df, col("AuthorID_lookup") == dim_author_loaded_df.AuthorID_NK, "left_outer") \
    .join(dim_topic_loaded_df, col("TopicID_lookup") == dim_topic_loaded_df.TopicID_NK, "left_outer") \
    .join(dim_subtopic_loaded_df, col("SubTopicID_lookup") == dim_subtopic_loaded_df.SubTopicID_NK, "left_outer") \
    .select(
        col("DateKey").alias("PublicationDateKey"),
        col("ArticlePublicationTimestamp"),
        col("AuthorKey"),
        col("TopicKey"),
        col("SubTopicKey"),
        col("ArticleID_NK"),
        col("ArticleTitle"),
        col("ArticleDescription"),
        lit(1).alias("PublishedArticleCount"),
        col("OpinionCount"),
        col("WordCountInMainContent"),
        col("CharacterCountInMainContent"),
        col("EstimatedReadTimeMinutes"),
        col("TaggedKeywordCountInArticle"),
        col("ReferenceSourceCountInArticle")
    ).na.fill({"PublicationDateKey": -1, "AuthorKey": -1, "TopicKey": -1, "SubTopicKey": -1})
write_curated_table_initial_load(fact_article_publication_df, "fact_article_publication", partition_cols=["ArticlePublicationTimestamp"], mode="append")


# 3.2. FactArticleKeyword
print("Đang xử lý fact_article_keyword...")
fact_pub_keys = fact_article_publication_df.select("ArticleID_NK", "PublicationDateKey", "AuthorKey", "TopicKey", "SubTopicKey") \
                    .withColumnRenamed("PublicationDateKey", "ArticlePublicationDateKey")

fact_article_keyword_df = article_keywords_clean_df \
    .join(fact_pub_keys, article_keywords_clean_df.ArticleID == fact_pub_keys.ArticleID_NK, "inner") \
    .join(dim_keyword_loaded_df, article_keywords_clean_df.KeywordID == dim_keyword_loaded_df.KeywordID_NK, "left_outer") \
    .select(
        col("ArticlePublicationDateKey"),
        col("ArticleID").alias("ArticleID_NK"),
        col("KeywordKey"),
        col("AuthorKey"),
        col("TopicKey"),
        col("SubTopicKey"),
        lit(1).alias("IsKeywordTaggedToArticle")
    ).na.fill(-1, subset=["ArticlePublicationDateKey", "KeywordKey", "AuthorKey", "TopicKey", "SubTopicKey"])
write_curated_table_initial_load(fact_article_keyword_df, "fact_article_keyword")


# 3.3. FactArticleReference
print("Đang xử lý fact_article_reference...")
fact_article_reference_df = article_references_clean_df \
    .join(fact_pub_keys, article_references_clean_df.ArticleID == fact_pub_keys.ArticleID_NK, "inner") \
    .join(dim_referencesource_loaded_df, article_references_clean_df.ReferenceID == dim_referencesource_loaded_df.ReferenceID_NK, "left_outer") \
    .select(
        col("ArticlePublicationDateKey"),
        col("ArticleID").alias("ArticleID_NK"),
        col("ReferenceSourceKey"),
        col("AuthorKey"),
        col("TopicKey"),
        col("SubTopicKey"),
        lit(1).alias("IsReferenceUsedInArticle")
    ).na.fill(-1, subset=["ArticlePublicationDateKey", "ReferenceSourceKey", "AuthorKey", "TopicKey", "SubTopicKey"])
write_curated_table_initial_load(fact_article_reference_df, "fact_article_reference")


# 3.4. FactTopCommentActivity
print("Đang xử lý fact_top_comment_activity...")
fact_top_comment_activity_df = comments_clean_df \
    .join(fact_pub_keys, comments_clean_df.ArticleID == fact_pub_keys.ArticleID_NK, "inner") \
    .select(
        col("ArticlePublicationDateKey"),
        col("ArticlePublicationDateKey").alias("CommentDateKey"),
        col("ArticleID").alias("ArticleID_NK"),
        col("CommentID").alias("CommentID_NK"),
        col("AuthorKey"),
        col("TopicKey"),
        col("SubTopicKey"),
        col("CommenterName"),
        lit(1).alias("IsTopComment"),
        col("TotalLikes").alias("LikesOnTopComment")
    ).na.fill({"ArticlePublicationDateKey": -1, "CommentDateKey": -1, "AuthorKey": -1, "TopicKey": -1, "SubTopicKey": -1})
write_curated_table_initial_load(fact_top_comment_activity_df, "fact_top_comment_activity")


# 3.5. FactTopCommentInteractionDetail
print("Đang xử lý fact_top_comment_interaction_detail...")
fact_top_comment_interaction_detail_df = comment_interactions_clean_df \
    .join(comments_clean_df.select("CommentID", "ArticleID"), ["CommentID"], "inner") \
    .join(fact_pub_keys, col("ArticleID") == fact_pub_keys.ArticleID_NK, "inner") \
    .join(dim_interactiontype_loaded_df, comment_interactions_clean_df.InteractionType == dim_interactiontype_loaded_df.InteractionTypeName, "left_outer") \
    .select(
        col("ArticlePublicationDateKey"),
        col("ArticlePublicationDateKey").alias("InteractionDateKey"),
        col("ArticleID").alias("ArticleID_NK"),
        col("CommentID").alias("CommentID_NK"),
        col("InteractionTypeKey"),
        col("AuthorKey"),
        col("TopicKey"),
        col("SubTopicKey"),
        lit(1).alias("InteractionInstanceCount"),
        col("InteractionCount").alias("InteractionValue")
    ).na.fill({"ArticlePublicationDateKey": -1, "InteractionDateKey": -1, "InteractionTypeKey": -1, "AuthorKey": -1, "TopicKey": -1, "SubTopicKey": -1})
write_curated_table_initial_load(fact_top_comment_interaction_detail_df, "fact_top_comment_interaction_detail")


print("\n--- Hoàn thành quy trình ETL INITIAL LOAD từ Clean sang Curated ---")
spark.stop()
