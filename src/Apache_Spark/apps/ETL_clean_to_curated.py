from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth, current_timestamp,
    quarter, date_format, countDistinct, count, sum as _sum, lit,
    when, expr, coalesce, avg, round as _round, length, split, size,
    row_number, xxhash64, min as _min, max as _max
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType, FloatType, LongType, BooleanType
from pyspark.sql.window import Window
from datetime import datetime, timedelta

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

app_name = "NewsETLCleanToCurated_V2"

# --- CẤU HÌNH THỜI GIAN ETL ---
ETL_START_DATE_STR = "2025-05-01"
ETL_END_DATE_STR = "2025-05-07"

spark_builder = SparkSession.builder.appName(app_name)

# --- Cấu hình SparkSession (S3A, Extensions, Catalogs) ---
spark_builder = spark_builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

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

print(f"SparkSession đã được khởi tạo cho: {app_name}.")

# --- HÀM TIỆN ÍCH ---
def get_date_range(start_date_str=None, end_date_str=None, default_days=7):
    date_format_str = "%Y-%m-%d"
    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, date_format_str)
            end_date = datetime.strptime(end_date_str, date_format_str)
            print(f"Sử dụng khoảng thời gian tùy chỉnh: {start_date.strftime(date_format_str)} đến {end_date.strftime(date_format_str)}")
            return start_date, end_date
        except ValueError:
            print(f"Lỗi định dạng ngày: {start_date_str}, {end_date_str}. Vui lòng dùng YYYY-MM-DD. Sử dụng khoảng mặc định.")

    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=default_days - 1)
    print(f"Sử dụng khoảng thời gian mặc định: {default_days} ngày, từ {start_date.strftime(date_format_str)} đến {end_date.strftime(date_format_str)}")
    return start_date, end_date

def read_iceberg_table(table_name, catalog_name, db_name, date_filter_col=None, start_date=None, end_date=None):
    full_table_name = f"`{catalog_name}`.`{db_name}`.`{table_name}`"
    print(f"Đang đọc từ bảng: {full_table_name}")
    try:
        df = spark.read.format("iceberg").load(full_table_name)
        if date_filter_col and start_date and end_date:
            print(f"Áp dụng bộ lọc ngày trên cột '{date_filter_col}': từ {start_date.strftime('%Y-%m-%d')} đến {end_date.strftime('%Y-%m-%d')}")
            df = df.filter(col(date_filter_col) >= lit(start_date.strftime('%Y-%m-%d')).cast(DateType())) \
                  .filter(col(date_filter_col) <= lit(end_date.strftime('%Y-%m-%d')).cast(DateType()))

        count_val = df.count()
        print(f"Đọc bảng {full_table_name} thành công. Số dòng: {count_val}")
        if count_val == 0 and date_filter_col :
            print(f"Lưu ý: Không có dữ liệu trong khoảng ngày đã chọn cho bảng {table_name}.")
        return df
    except Exception as e:
        print(f"Lỗi khi đọc bảng {full_table_name}: {e}")
        raise

def write_curated_iceberg_table(df_to_write, table_name, write_mode,
                                 primary_key_cols_for_merge=None,
                                 partition_cols=None,
                                 catalog_name=curated_catalog_name, db_name=CURATED_DATABASE_NAME):
    full_table_name = f"`{catalog_name}`.`{db_name}`.`{table_name}`"
    temp_view_name = f"{table_name}_source_view_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

    if df_to_write.rdd.isEmpty():
        print(f"Không có dữ liệu để ghi vào bảng {full_table_name} với chế độ '{write_mode}'. Bỏ qua.")
        return

    print(f"Chuẩn bị ghi vào bảng Curated: {full_table_name} (Mode: {write_mode})")
    df_to_write.createOrReplaceTempView(temp_view_name)

    try:
        table_exists = spark.sql(f"SHOW TABLES IN `{catalog_name}`.`{db_name}` LIKE '{table_name.split('.')[-1]}'").count() > 0

        if write_mode == "merge":
            if not primary_key_cols_for_merge:
                raise ValueError("Chế độ 'merge' yêu cầu 'primary_key_cols_for_merge'.")
            if not table_exists:
                print(f"Bảng {full_table_name} chưa tồn tại. Tạo bảng và ghi dữ liệu (thay vì merge).")
                writer_ops = df_to_write.writeTo(full_table_name).tableProperty("write.format.default", "parquet")
                if partition_cols:
                    writer_ops = writer_ops.partitionedBy(*partition_cols)
                writer_ops.create()
            else:
                merge_condition = " AND ".join([f"target.{pk_col} = source.{pk_col}" for pk_col in primary_key_cols_for_merge])
                merge_sql = f"""
                MERGE INTO {full_table_name} AS target
                USING {temp_view_name} AS source
                ON {merge_condition}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
                print(f"Thực thi MERGE SQL cho {full_table_name}: \n{merge_sql}")
                spark.sql(merge_sql)

        elif write_mode == "overwrite_dynamic_partitions":
            if not partition_cols:
                raise ValueError("Chế độ 'overwrite_dynamic_partitions' yêu cầu 'partition_cols'.")
            print(f"Thực hiện overwrite_dynamic_partitions cho {full_table_name} với các cột partition: {partition_cols}")
            df_to_write.write.format("iceberg") \
                .mode("overwrite") \
                .option("overwrite-mode", "dynamic") \
                .save(full_table_name)

        elif write_mode == "overwrite_table":
            writer_ops = df_to_write.writeTo(full_table_name).tableProperty("write.format.default", "parquet")
            if partition_cols:
                writer_ops = writer_ops.partitionedBy(*partition_cols)
            writer_ops.createOrReplace()

        else:
            raise ValueError(f"Chế độ ghi '{write_mode}' không được hỗ trợ.")

        print(f"Ghi (Mode: {write_mode}) vào bảng {full_table_name} thành công.")

    except Exception as e:
        print(f"Lỗi khi ghi (Mode: {write_mode}) vào bảng {full_table_name}: {e}")
        raise
    finally:
        spark.catalog.dropTempView(temp_view_name)

# --- 0. Xác định khoảng ngày xử lý ---
start_date_process, end_date_process = get_date_range(ETL_START_DATE_STR, ETL_END_DATE_STR)

# --- 1. Đọc dữ liệu từ vùng CLEAN ---
print("\n--- Bước 1: Đọc dữ liệu từ vùng CLEAN ---")
authors_clean_df = read_iceberg_table("authors", clean_catalog_name, CLEAN_DATABASE_NAME)
topics_clean_df = read_iceberg_table("topics", clean_catalog_name, CLEAN_DATABASE_NAME)
subtopics_clean_df = read_iceberg_table("subtopics", clean_catalog_name, CLEAN_DATABASE_NAME)
keywords_clean_df = read_iceberg_table("keywords", clean_catalog_name, CLEAN_DATABASE_NAME)
references_clean_df = read_iceberg_table("references_table", clean_catalog_name, CLEAN_DATABASE_NAME)
base_comment_interactions_clean_df = read_iceberg_table("comment_interactions", clean_catalog_name, CLEAN_DATABASE_NAME)

articles_clean_df = read_iceberg_table("articles", clean_catalog_name, CLEAN_DATABASE_NAME,
                                       date_filter_col="PublicationDate",
                                       start_date=start_date_process, end_date=end_date_process)

if articles_clean_df.rdd.isEmpty():
    print(f"Không có bài báo nào trong khoảng từ {start_date_process.strftime('%Y-%m-%d')} đến {end_date_process.strftime('%Y-%m-%d')}. Dừng ETL.")
    spark.stop()
    exit(0)

processed_article_ids_df = articles_clean_df.select("ArticleID").distinct().cache()

article_keywords_clean_df = read_iceberg_table("article_keywords", clean_catalog_name, CLEAN_DATABASE_NAME) \
    .join(processed_article_ids_df, ["ArticleID"], "inner")
article_references_clean_df = read_iceberg_table("article_references", clean_catalog_name, CLEAN_DATABASE_NAME) \
    .join(processed_article_ids_df, ["ArticleID"], "inner")
comments_clean_df = read_iceberg_table("comments", clean_catalog_name, CLEAN_DATABASE_NAME) \
    .join(processed_article_ids_df, ["ArticleID"], "inner").cache()

processed_comment_ids_df = comments_clean_df.select("CommentID").distinct()
comment_interactions_for_facts_df = base_comment_interactions_clean_df.join(processed_comment_ids_df, ["CommentID"], "inner")

# --- 2. Xây dựng Bảng Chiều (Dimension Tables) cho CURATED ---
print("\n--- Bước 2: Xây dựng Dimension Tables cho CURATED ---")
current_ts = current_timestamp()

print("Đang đọc dim_date (đã được populate trước đó)...")
dim_date_df = read_iceberg_table("dim_date", curated_catalog_name, CURATED_DATABASE_NAME)
if dim_date_df.rdd.isEmpty():
    print("LỖI NGHIÊM TRỌNG: Bảng dim_date rỗng hoặc không tồn tại. Dừng ETL.")
    spark.stop()
    exit(1)

print("Đang xử lý dim_author...")
dim_author_df = authors_clean_df \
    .withColumn("AuthorKey", xxhash64(col("AuthorID"))) \
    .select(col("AuthorKey"), col("AuthorID").alias("AuthorID_NK"), col("AuthorName")) \
    .withColumn("LastUpdatedTimestamp_Curated", current_ts) \
    .dropDuplicates(["AuthorID_NK"])
write_curated_iceberg_table(dim_author_df, "dim_author", write_mode="merge", primary_key_cols_for_merge=["AuthorID_NK"])
dim_author_df.cache()

print("Đang xử lý dim_topic...")
dim_topic_df = topics_clean_df \
    .withColumn("TopicKey", xxhash64(col("TopicID"))) \
    .select(col("TopicKey"), col("TopicID").alias("TopicID_NK"), col("TopicName")) \
    .withColumn("LastUpdatedTimestamp_Curated", current_ts) \
    .dropDuplicates(["TopicID_NK"])
write_curated_iceberg_table(dim_topic_df, "dim_topic", write_mode="merge", primary_key_cols_for_merge=["TopicID_NK"])
dim_topic_df.cache()

print("Đang xử lý dim_sub_topic...")
dim_subtopic_df = subtopics_clean_df \
    .join(dim_topic_df, subtopics_clean_df.TopicID == dim_topic_df.TopicID_NK, "left_outer") \
    .withColumn("SubTopicKey", xxhash64(col("SubTopicID"))) \
    .select(
        col("SubTopicKey"),
        col("SubTopicID").alias("SubTopicID_NK"),
        col("SubTopicName"), # Giữ nguyên tên cột SubTopicName
        col("TopicKey").alias("ParentTopicKey"),
        col("TopicName").alias("ParentTopicName")
    ) \
    .withColumn("LastUpdatedTimestamp_Curated", current_ts) \
    .dropDuplicates(["SubTopicID_NK"])
write_curated_iceberg_table(dim_subtopic_df, "dim_sub_topic", write_mode="merge", primary_key_cols_for_merge=["SubTopicID_NK"])
dim_subtopic_df.cache()

print("Đang xử lý dim_keyword...")
dim_keyword_df = keywords_clean_df \
    .withColumn("KeywordKey", xxhash64(col("KeywordID"))) \
    .select(col("KeywordKey"), col("KeywordID").alias("KeywordID_NK"), col("KeywordText")) \
    .withColumn("LastUpdatedTimestamp_Curated", current_ts) \
    .dropDuplicates(["KeywordID_NK"])
write_curated_iceberg_table(dim_keyword_df, "dim_keyword", write_mode="merge", primary_key_cols_for_merge=["KeywordID_NK"])
dim_keyword_df.cache()

print("Đang xử lý dim_reference_source...")
dim_referencesource_df = references_clean_df \
    .withColumn("ReferenceSourceKey", xxhash64(col("ReferenceID"))) \
    .select(col("ReferenceSourceKey"), col("ReferenceID").alias("ReferenceID_NK"), col("ReferenceText")) \
    .withColumn("LastUpdatedTimestamp_Curated", current_ts) \
    .dropDuplicates(["ReferenceID_NK"])
write_curated_iceberg_table(dim_referencesource_df, "dim_reference_source", write_mode="merge", primary_key_cols_for_merge=["ReferenceID_NK"])
dim_referencesource_df.cache()

print("Đang xử lý dim_interaction_type...")
dim_interactiontype_df = base_comment_interactions_clean_df \
    .select(col("InteractionType").alias("InteractionTypeName")) \
    .filter(col("InteractionTypeName").isNotNull()) \
    .distinct() \
    .withColumn("InteractionTypeKey", xxhash64(col("InteractionTypeName"))) \
    .select("InteractionTypeKey", "InteractionTypeName") \
    .withColumn("LastUpdatedTimestamp_Curated", current_ts) \
    .dropDuplicates(["InteractionTypeName"])
write_curated_iceberg_table(dim_interactiontype_df, "dim_interaction_type", write_mode="merge", primary_key_cols_for_merge=["InteractionTypeName"])
dim_interactiontype_df.cache()

# --- 3. Xây dựng Bảng Sự kiện (Fact Tables) cho CURATED ---
print("\n--- Bước 3: Xây dựng Fact Tables cho CURATED ---")

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
    .join(dim_date_df, col("PublicationDateOnly") == dim_date_df.FullDateAlternateKey, "left_outer") \
    .join(dim_author_df, col("AuthorID_lookup") == dim_author_df.AuthorID_NK, "left_outer") \
    .join(dim_topic_df, col("TopicID_lookup") == dim_topic_df.TopicID_NK, "left_outer") \
    .join(dim_subtopic_df, col("SubTopicID_lookup") == dim_subtopic_df.SubTopicID_NK, "left_outer") \
    .select(
        col("DateKey").alias("PublicationDateKey"),
        col("ArticlePublicationTimestamp"), # Cột này sẽ được dùng để partition
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
write_curated_iceberg_table(fact_article_publication_df, "fact_article_publication",
                            write_mode="overwrite_dynamic_partitions",
                            partition_cols=["ArticlePublicationTimestamp"])

fact_pub_keys_df = fact_article_publication_df.select(
    "ArticleID_NK",
    col("PublicationDateKey").alias("ArticlePublicationDateKey"),
    "AuthorKey", "TopicKey", "SubTopicKey"
).distinct().cache()

# --- 3.2. FactArticleKeyword ---
print("Đang xử lý fact_article_keyword...")
fact_article_keyword_df = article_keywords_clean_df \
    .join(fact_pub_keys_df, article_keywords_clean_df.ArticleID == fact_pub_keys_df.ArticleID_NK, "inner") \
    .join(dim_keyword_df.select("KeywordID_NK", "KeywordKey"), article_keywords_clean_df.KeywordID == dim_keyword_df.KeywordID_NK, "left_outer") \
    .select(
        col("ArticlePublicationDateKey"),
        article_keywords_clean_df.ArticleID.alias("ArticleID_NK"),
        col("KeywordKey"),
        col("AuthorKey"),
        col("TopicKey"),
        col("SubTopicKey"),
        lit(1).alias("IsKeywordTaggedToArticle")
    ).na.fill(-1, subset=["ArticlePublicationDateKey", "KeywordKey", "AuthorKey", "TopicKey", "SubTopicKey"])
if not fact_article_keyword_df.rdd.isEmpty():
    write_curated_iceberg_table(fact_article_keyword_df, "fact_article_keyword",
                                write_mode="overwrite_dynamic_partitions",
                                partition_cols=["ArticlePublicationDateKey"])
else:
    print("Không có dữ liệu cho fact_article_keyword.")

# --- 3.3. FactArticleReference ---
print("Đang xử lý fact_article_reference...")
fact_article_reference_df = article_references_clean_df \
    .join(fact_pub_keys_df, article_references_clean_df.ArticleID == fact_pub_keys_df.ArticleID_NK, "inner") \
    .join(dim_referencesource_df, article_references_clean_df.ReferenceID == dim_referencesource_df.ReferenceID_NK, "left_outer") \
    .select(
        col("ArticlePublicationDateKey"),
        article_references_clean_df.ArticleID.alias("ArticleID_NK"),
        col("ReferenceSourceKey"),
        col("AuthorKey"),
        col("TopicKey"),
        col("SubTopicKey"),
        lit(1).alias("IsReferenceUsedInArticle")
    ).na.fill(-1, subset=["ArticlePublicationDateKey", "ReferenceSourceKey", "AuthorKey", "TopicKey", "SubTopicKey"])
if not fact_article_reference_df.rdd.isEmpty():
    write_curated_iceberg_table(fact_article_reference_df, "fact_article_reference",
                                write_mode="overwrite_dynamic_partitions",
                                partition_cols=["ArticlePublicationDateKey"])
else:
    print("Không có dữ liệu cho fact_article_reference.")

# --- 3.4. FactTopCommentActivity ---
print("Đang xử lý fact_top_comment_activity...")
fact_top_comment_activity_df = comments_clean_df \
    .join(fact_pub_keys_df, comments_clean_df.ArticleID == fact_pub_keys_df.ArticleID_NK, "inner") \
    .select(
        col("ArticlePublicationDateKey"),
        col("ArticlePublicationDateKey").alias("CommentDateKey"), # Giả định ngày comment trùng ngày đăng bài
        comments_clean_df.ArticleID.alias("ArticleID_NK"),
        comments_clean_df.CommentID.alias("CommentID_NK"),
        col("AuthorKey"),
        col("TopicKey"),
        col("SubTopicKey"),
        col("CommenterName"),
        lit(1).alias("IsTopComment"),
        col("TotalLikes").alias("LikesOnTopComment")
    ).na.fill({"ArticlePublicationDateKey": -1, "CommentDateKey": -1, "AuthorKey": -1, "TopicKey": -1, "SubTopicKey": -1})
if not fact_top_comment_activity_df.rdd.isEmpty():
    write_curated_iceberg_table(fact_top_comment_activity_df, "fact_top_comment_activity",
                                write_mode="overwrite_dynamic_partitions",
                                partition_cols=["ArticlePublicationDateKey"])
else:
    print("Không có dữ liệu cho fact_top_comment_activity.")

# --- 3.5. FactTopCommentInteractionDetail ---
print("Đang xử lý fact_top_comment_interaction_detail...")
fact_top_comment_interaction_detail_df = comment_interactions_for_facts_df.alias("cif") \
    .join(comments_clean_df.select("CommentID", "ArticleID").alias("cmt"),
          col("cif.CommentID") == col("cmt.CommentID"), "inner") \
    .join(fact_pub_keys_df.alias("fpk"),
          col("cmt.ArticleID") == col("fpk.ArticleID_NK"), "inner") \
    .join(dim_interactiontype_df.select("InteractionTypeName", "InteractionTypeKey").alias("dit"),
          col("cif.InteractionType") == col("dit.InteractionTypeName"), "left_outer") \
    .select(
        col("fpk.ArticlePublicationDateKey").alias("ArticlePublicationDateKey"),
        col("fpk.ArticlePublicationDateKey").alias("InteractionDateKey"), # Giả định ngày tương tác trùng ngày đăng bài
        col("cmt.ArticleID").alias("ArticleID_NK"),
        col("cif.CommentID").alias("CommentID_NK"),
        col("dit.InteractionTypeKey"),
        col("fpk.AuthorKey"),
        col("fpk.TopicKey"),
        col("fpk.SubTopicKey"),
        lit(1).alias("InteractionInstanceCount"),
        col("cif.InteractionCount").alias("InteractionValue")
    ).na.fill({
        "ArticlePublicationDateKey": -1,
        "InteractionDateKey": -1,
        "InteractionTypeKey": -1,
        "AuthorKey": -1,
        "TopicKey": -1,
        "SubTopicKey": -1
    })

if not fact_top_comment_interaction_detail_df.rdd.isEmpty():
    write_curated_iceberg_table(
        fact_top_comment_interaction_detail_df,
        "fact_top_comment_interaction_detail",
        write_mode="overwrite_dynamic_partitions",
        partition_cols=["ArticlePublicationDateKey"]
    )
else:
    print("Không có dữ liệu cho fact_top_comment_interaction_detail.")

# Giải phóng các DataFrame đã cache
processed_article_ids_df.unpersist()
comments_clean_df.unpersist()
dim_author_df.unpersist()
dim_topic_df.unpersist()
dim_subtopic_df.unpersist()
dim_keyword_df.unpersist()
dim_referencesource_df.unpersist()
dim_interactiontype_df.unpersist()
fact_pub_keys_df.unpersist()

print(f"\n--- Hoàn thành quy trình ETL từ Clean sang Curated (V2) cho khoảng ngày {start_date_process.strftime('%Y-%m-%d')} đến {end_date_process.strftime('%Y-%m-%d')} ---")
spark.stop()
