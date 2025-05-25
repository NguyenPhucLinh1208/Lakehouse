from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, monotonically_increasing_id, to_date, year, month, dayofmonth,
    quarter, date_format, countDistinct, count, sum as _sum, lit,
    when, expr, coalesce, avg, round as _round, length, split, size,
    current_date, date_sub # Thêm cho xử lý ngày
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType, FloatType, LongType, BooleanType
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# --- Cấu hình (Tương tự initial load) ---
minio_endpoint = "http://minio1:9000"
minio_access_key = "rH4arFLYBxl55rh2zmN1"
minio_secret_key = "AtEB2XiMAznxvJXRvaXxigdIewIMVKscgPg7dJjI"
nessie_uri = "http://nessie:19120/api/v2"
nessie_default_branch = "main"
clean_catalog_name = "nessie-clean-news"
clean_catalog_warehouse_path = "s3a://clean-news-lakehouse/nessie_clean_news_warehouse"
CLEAN_DATABASE_NAME = "news_clean_db"
curated_catalog_name = "nessie-curated-news"
curated_catalog_warehouse_path = "s3a://curated-news-lakehouse/nessie_curated_news_warehouse"
CURATED_DATABASE_NAME = "news_curated_db"
app_name = "DailyUpdateCleanToCuratedNewsETL"

spark_builder = SparkSession.builder.appName(app_name)
# Cấu hình SparkSession (Tương tự initial load, bao gồm cả 2 catalogs)
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
print("SparkSession đã được khởi tạo cho Daily Update: Clean -> Curated.")

# Hàm trợ giúp đọc và ghi
def read_clean_table(table_name, catalog=clean_catalog_name, db=CLEAN_DATABASE_NAME, date_filter_col=None, days_to_load=7):
    full_table_name = f"`{catalog}`.`{db}`.`{table_name}`"
    print(f"Đang đọc từ bảng Clean: {full_table_name}")
    try:
        df = spark.read.format("iceberg").load(full_table_name)
        if date_filter_col:
            # Lấy dữ liệu cho `days_to_load` ngày gần nhất (bao gồm ngày hôm nay)
            end_date = spark.sql("SELECT current_date()").first()[0]
            start_date = spark.sql(f"SELECT date_sub(to_date('{end_date.strftime('%Y-%m-%d')}'), {days_to_load - 1})").first()[0]
            
            print(f"Lọc dữ liệu bảng {table_name} từ {start_date.strftime('%Y-%m-%d')} đến {end_date.strftime('%Y-%m-%d')} dựa trên cột {date_filter_col}")
            df = df.filter(to_date(col(date_filter_col)) >= start_date).filter(to_date(col(date_filter_col)) <= end_date)
        
        count = df.count() # Đếm sau khi lọc nếu có
        print(f"Đọc bảng {full_table_name} thành công. Số dòng (sau khi lọc nếu có): {count}")
        if count == 0 and date_filter_col:
             print(f"Cảnh báo: Không có dữ liệu mới trong {days_to_load} ngày qua cho {table_name} dựa trên {date_filter_col}.")
        return df
    except Exception as e:
        print(f"Lỗi khi đọc bảng {full_table_name} từ Clean: {e}")
        raise e

def merge_into_curated_dimension(df_new_data, dim_table_name, nk_col_name, sk_col_name, attribute_cols_to_update):
    """Thực hiện MERGE INTO cho bảng Dimension (SCD Type 1)."""
    full_dim_table_name = f"`{curated_catalog_name}`.`{CURATED_DATABASE_NAME}`.`{dim_table_name}`"
    temp_view_name = f"new_dim_{dim_table_name}_source"
    df_new_data.createOrReplaceTempView(temp_view_name)

    # Xây dựng mệnh đề SET cho UPDATE (chỉ update nếu giá trị khác)
    set_clauses = []
    condition_clauses = []
    for attr_col in attribute_cols_to_update:
        set_clauses.append(f"target.{attr_col} = source.{attr_col}")
        condition_clauses.append(f"NOT (target.{attr_col} <=> source.{attr_col})") # <=> an toàn với NULL
    
    update_set_sql = ", ".join(set_clauses)
    update_condition_sql = " OR ".join(condition_clauses) if condition_clauses else "1 = 0" # Nếu không có cột attr, không update

    # Xây dựng danh sách cột cho INSERT
    all_cols = [sk_col_name, nk_col_name] + attribute_cols_to_update
    insert_cols_sql = ", ".join(all_cols)
    insert_values_sql = ", ".join([f"source.{col}" for col in all_cols])
    
    # Tạo SK cho dữ liệu mới nếu nó chưa có (df_new_data chỉ chứa NK và attributes)
    # Giả định df_new_data đã được chuẩn bị với các cột giống Dim, trừ SK
    # Cần logic tạo SK cho các bản ghi mới.
    # Để đơn giản, ta đọc Dim hiện tại, tìm max SK, rồi gán SK cho các bản ghi mới.
    # Hoặc, nếu df_new_data đã được gán SK (ví dụ, bằng cách join với Dim và chỉ lấy phần mới), thì không cần.
    
    # Cách tiếp cận đơn giản: MERGE dựa trên NK, chỉ INSERT nếu NOT MATCHED.
    # Nếu MATCHED, giả định không có thay đổi thuộc tính đáng kể hoặc xử lý SCD1 đơn giản.
    # Lấy tên các cột của df_new_data (giả định nó đã có các cột tương ứng với Dim, trừ SK)
    source_cols = df_new_data.columns
    insert_dim_cols_sql = ", ".join([sk_col_name] + source_cols) # sk_col_name, nk_col_name, attr1, attr2...
    
    # Lấy max SK hiện tại từ Dimension để tạo SK mới
    try:
        max_sk_row = spark.sql(f"SELECT MAX({sk_col_name}) as max_sk FROM {full_dim_table_name}").first()
        current_max_sk = max_sk_row["max_sk"] if max_sk_row and max_sk_row["max_sk"] is not None else 0
    except: # Bảng có thể chưa tồn tại hoặc rỗng
        current_max_sk = 0
        print(f"Không tìm thấy max SK từ {full_dim_table_name}, bắt đầu SK từ 0.")

    # Thêm cột SK mới vào source view, đảm bảo nó duy nhất và tăng dần
    # Đây là cách tạo SK đơn giản, không hoàn toàn an toàn với chạy song song, chỉ cho demo
    df_new_data_with_sk_potential = df_new_data.withColumn(sk_col_name, monotonically_increasing_id() + current_max_sk + 1)
    df_new_data_with_sk_potential.createOrReplaceTempView(temp_view_name) # Ghi đè temp view với SK

    insert_dim_values_sql = ", ".join([f"source.{col}" for col in df_new_data_with_sk_potential.columns]) # Giờ source đã có SK

    merge_sql = f"""
    MERGE INTO {full_dim_table_name} target
    USING {temp_view_name} source
    ON target.{nk_col_name} = source.{nk_col_name}
    WHEN MATCHED AND ({update_condition_sql}) -- Chỉ update nếu có thay đổi thực sự
    THEN UPDATE SET {update_set_sql}
    WHEN NOT MATCHED
    THEN INSERT ({insert_cols_sql}) VALUES ({insert_dim_values_sql}) 
    """
    # Phiên bản đơn giản hơn nếu chỉ insert, không update (SCD0 hoặc khi các thuộc tính không đổi)
    # merge_sql = f"""
    # MERGE INTO {full_dim_table_name} target
    # USING {temp_view_name} source
    # ON target.{nk_col_name} = source.{nk_col_name}
    # WHEN NOT MATCHED
    # THEN INSERT ({insert_cols_sql}) VALUES ({insert_dim_values_sql})
    # """

    print(f"Đang MERGE dữ liệu vào Dimension: {full_dim_table_name}")
    try:
        print(f"Executing MERGE SQL: \n{merge_sql}")
        spark.sql(merge_sql)
        print(f"MERGE dữ liệu vào bảng '{full_dim_table_name}' thành công.")
    except Exception as e:
        print(f"Lỗi khi MERGE vào bảng '{full_dim_table_name}': {e}")
        raise e
    finally:
        spark.catalog.dropTempView(temp_view_name)


def write_curated_fact_table_daily(df_to_write, fact_table_name, mode, partition_cols=None, delete_condition_col=None, delete_values_df=None):
    """Ghi vào Fact table với logic overwritePartitions hoặc delete/append."""
    full_fact_table_name = f"`{curated_catalog_name}`.`{CURATED_DATABASE_NAME}`.`{fact_table_name}`"
    print(f"Đang ghi (Daily Update) vào Fact: {full_fact_table_name} với mode '{mode}'")
    try:
        writer = df_to_write.writeTo(full_fact_table_name) \
            .tableProperty("write.format.default", "parquet")

        if mode == "overwrite_partitions":
            if not partition_cols:
                raise ValueError("partition_cols là bắt buộc cho 'overwrite_partitions'")
            # Cần đảm bảo df_to_write có các cột phân vùng
            # và Iceberg biết cách map chúng với transform phân vùng của bảng
            # Ví dụ, nếu bảng phân vùng bằng days(timestamp_col), df_to_write phải có cột tương ứng
            # hoặc bạn truyền transform vào partitionedBy nếu API hỗ trợ
            
            # Tạo cột ngày từ timestamp để Iceberg có thể map với days(ArticlePublicationTimestamp)
            # Giả sử partition_cols chỉ là tên cột timestamp gốc
            # và bảng được phân vùng bằng days(tên_cột_đó)
            # Cách an toàn là chuẩn bị sẵn cột phân vùng trong df_to_write
            # Ví dụ: df_to_write = df_to_write.withColumn("partition_day_col", days(col(partition_cols[0])))
            # writer = writer.partitionedBy("partition_day_col")
            
            # Giả sử df_to_write đã chứa dữ liệu chỉ cho các phân vùng cần ghi đè
            # và tên cột trong partition_cols khớp với cột trong df_to_write dùng để xác định phân vùng
            if partition_cols: # Nếu có partition_cols, dùng nó
                 writer = writer.partitionedBy(*partition_cols)
            writer.overwritePartitions()

        elif mode == "delete_append":
            if not delete_condition_col or delete_values_df is None or delete_values_df.count() == 0:
                print(f"Không có giá trị điều kiện xóa cho {fact_table_name} hoặc delete_values_df rỗng. Bỏ qua bước DELETE.")
            else:
                values_to_delete_rows = delete_values_df.select(delete_condition_col).distinct().collect()
                values_to_delete = [row[delete_condition_col] for row in values_to_delete_rows]
                
                if values_to_delete:
                    # Chia thành các batch nhỏ nếu danh sách quá dài cho mệnh đề IN
                    batch_size = 200 # Tùy chỉnh batch size
                    for i in range(0, len(values_to_delete), batch_size):
                        batch_values = values_to_delete[i:i + batch_size]
                        values_str = ", ".join([f"'{val}'" if isinstance(val, str) else str(val) for val in batch_values])
                        delete_sql = f"DELETE FROM {full_fact_table_name} WHERE {delete_condition_col} IN ({values_str})"
                        print(f"Thực hiện DELETE (batch): {delete_sql}")
                        spark.sql(delete_sql)
                    print(f"Đã DELETE dữ liệu cũ từ {full_fact_table_name} cho {delete_condition_col} liên quan.")
                else:
                    print(f"Không có giá trị cụ thể nào để DELETE trong {full_fact_table_name} cho {delete_condition_col}.")
            
            # Sau đó append dữ liệu mới
            if df_to_write.count() > 0:
                writer.append()
                print(f"Đã APPEND dữ liệu mới vào {full_fact_table_name}.")
            else:
                print(f"Không có dữ liệu mới để APPEND vào {full_fact_table_name}.")
        else:
            raise ValueError(f"Chế độ ghi '{mode}' không được hỗ trợ cho Daily Update Fact.")
        
        print(f"Ghi dữ liệu vào bảng '{full_fact_table_name}' thành công.")
    except Exception as e:
        print(f"Lỗi khi ghi vào bảng '{full_fact_table_name}': {e}")
        raise e

# --- 1. Đọc dữ liệu 7 ngày gần nhất từ vùng CLEAN ---
print("\n--- Bước 1: Đọc dữ liệu 7 ngày gần nhất từ vùng CLEAN ---")
# articles_clean_df_daily sẽ chứa dữ liệu PublicationDate trong 7 ngày gần nhất
articles_clean_df_daily = read_clean_table("articles", date_filter_col="PublicationDate", days_to_load=7)

if articles_clean_df_daily is None or articles_clean_df_daily.count() == 0:
    print("Không có dữ liệu bài viết mới trong 7 ngày qua từ Clean. Kết thúc job.")
    spark.stop()
    exit(0)

# Lấy danh sách ArticleID và DateKey cho 7 ngày này để lọc các bảng khác
article_ids_7_days_df = articles_clean_df_daily.select(col("ArticleID").alias("ArticleID_NK_filter")).distinct()

# Đọc toàn bộ các bảng dimension từ clean để xử lý MERGE
authors_clean_all_df = read_clean_table("authors", date_filter_col=None) # Đọc hết để merge
topics_clean_all_df = read_clean_table("topics", date_filter_col=None)
subtopics_clean_all_df = read_clean_table("subtopics", date_filter_col=None)
keywords_clean_all_df = read_clean_table("keywords", date_filter_col=None)
references_clean_all_df = read_clean_table("references_table", date_filter_col=None)
comment_interactions_clean_all_df = read_clean_table("comment_interactions", date_filter_col=None) # Để lấy distinct InteractionType

# Đọc các bảng phụ thuộc đã được lọc theo ArticleID của 7 ngày
article_keywords_clean_daily_df = read_clean_table("article_keywords").join(article_ids_7_days_df, col("ArticleID") == col("ArticleID_NK_filter"), "inner").drop("ArticleID_NK_filter")
article_references_clean_daily_df = read_clean_table("article_references").join(article_ids_7_days_df, col("ArticleID") == col("ArticleID_NK_filter"), "inner").drop("ArticleID_NK_filter")
comments_clean_daily_df = read_clean_table("comments").join(article_ids_7_days_df, col("ArticleID") == col("ArticleID_NK_filter"), "inner").drop("ArticleID_NK_filter")

# Lọc comment_interactions dựa trên CommentID từ comments_clean_daily_df
comment_ids_daily_df = comments_clean_daily_df.select("CommentID").distinct()
comment_interactions_clean_daily_df = read_clean_table("comment_interactions").join(comment_ids_daily_df, ["CommentID"], "inner")


# --- 2. Cập nhật Dimension Tables (MERGE INTO) ---
print("\n--- Bước 2: Cập nhật Dimension Tables (MERGE INTO) ---")
# 2.1. DimDate - Không cập nhật hàng ngày

# 2.2. DimAuthor
print("Đang xử lý DimAuthor (MERGE)...")
# Lấy authors mới từ articles_clean_df_daily (chỉ những tác giả có bài trong 7 ngày)
new_authors_data = articles_clean_df_daily.select(col("AuthorID").alias("AuthorID_NK"), col("tac_gia").alias("AuthorName")) \
    .filter(col("AuthorID_NK").isNotNull() & col("AuthorName").isNotNull()).distinct()
if new_authors_data.count() > 0:
    merge_into_curated_dimension(new_authors_data, "DimAuthor", "AuthorID_NK", "AuthorKey", ["AuthorName"])

# 2.3. DimTopic
print("Đang xử lý DimTopic (MERGE)...")
new_topics_data = articles_clean_df_daily.select(col("TopicID").alias("TopicID_NK"), col("topic").alias("TopicName")) \
    .filter(col("TopicID_NK").isNotNull() & col("TopicName").isNotNull()).distinct()
if new_topics_data.count() > 0:
    merge_into_curated_dimension(new_topics_data, "DimTopic", "TopicID_NK", "TopicKey", ["TopicName"])

# 2.4. DimSubTopic - Cần đọc lại DimTopic từ curated để lấy ParentTopicKey
print("Đang xử lý DimSubTopic (MERGE)...")
dim_topic_curated_df = read_clean_table("DimTopic", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME) # Đọc lại
new_subtopics_data_intermediate = articles_clean_df_daily \
    .filter(col("SubTopicID").isNotNull() & col("sub_topic").isNotNull() & col("TopicID").isNotNull()) \
    .select(col("SubTopicID").alias("SubTopicID_NK"), col("sub_topic").alias("SubTopicName"), col("TopicID").alias("ParentTopicID_NK_lookup")) \
    .distinct()
if new_subtopics_data_intermediate.count() > 0 :
    new_subtopics_data = new_subtopics_data_intermediate \
        .join(dim_topic_curated_df, col("ParentTopicID_NK_lookup") == dim_topic_curated_df.TopicID_NK, "left_outer") \
        .select("SubTopicID_NK", "SubTopicName", col("TopicKey").alias("ParentTopicKey"), col("TopicName").alias("ParentTopicName"))
    merge_into_curated_dimension(new_subtopics_data, "DimSubTopic", "SubTopicID_NK", "SubTopicKey", ["SubTopicName", "ParentTopicKey", "ParentTopicName"])

# 2.5. DimKeyword
print("Đang xử lý DimKeyword (MERGE)...")
new_keywords_data = article_keywords_clean_daily_df \
    .join(keywords_clean_all_df, article_keywords_clean_daily_df.KeywordID == keywords_clean_all_df.KeywordID, "inner") \
    .select(col("KeywordID").alias("KeywordID_NK"), col("KeywordText")).distinct()
if new_keywords_data.count() > 0:
    merge_into_curated_dimension(new_keywords_data, "DimKeyword", "KeywordID_NK", "KeywordKey", ["KeywordText"])

# 2.6. DimReferenceSource
print("Đang xử lý DimReferenceSource (MERGE)...")
new_references_data = article_references_clean_daily_df \
    .join(references_clean_all_df, article_references_clean_daily_df.ReferenceID == references_clean_all_df.ReferenceID, "inner") \
    .select(col("ReferenceID").alias("ReferenceID_NK"), col("ReferenceText")).distinct()
if new_references_data.count() > 0:
    merge_into_curated_dimension(new_references_data, "DimReferenceSource", "ReferenceID_NK", "ReferenceSourceKey", ["ReferenceText"])

# 2.7. DimInteractionType
print("Đang xử lý DimInteractionType (MERGE)...")
# Lấy từ toàn bộ comment_interactions_clean_all_df để đảm bảo có tất cả các loại
new_interaction_types_data = comment_interactions_clean_all_df \
    .select(col("InteractionType").alias("InteractionTypeName")) \
    .filter(col("InteractionTypeName").isNotNull()).distinct()
if new_interaction_types_data.count() > 0:
     merge_into_curated_dimension(new_interaction_types_data, "DimInteractionType", "InteractionTypeName", "InteractionTypeKey", []) # Không có cột attr để update ngoài key

# --- 3. Xây dựng Bảng Sự kiện (Fact Tables) cho CURATED (Daily Update) ---
# Đọc lại các Dimension từ Curated vì chúng có thể đã được cập nhật / thêm SK mới
print("\n--- Bước 3: Xây dựng Fact Tables cho CURATED (Daily Update) ---")
print("Đọc lại các Dimension từ Curated sau khi MERGE...")
dim_date_loaded_df = read_clean_table("DimDate", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME)
dim_author_loaded_df = read_clean_table("DimAuthor", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME)
dim_topic_loaded_df = read_clean_table("DimTopic", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME)
dim_subtopic_loaded_df = read_clean_table("DimSubTopic", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME)
dim_keyword_loaded_df = read_clean_table("DimKeyword", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME)
dim_referencesource_loaded_df = read_clean_table("DimReferenceSource", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME)
dim_interactiontype_loaded_df = read_clean_table("DimInteractionType", catalog=curated_catalog_name, db=CURATED_DATABASE_NAME)

# 3.1. FactArticlePublication (overwritePartitions)
print("Đang xử lý FactArticlePublication (overwritePartitions)...")
# Tính toán measures cho articles_clean_df_daily (dữ liệu 7 ngày)
tagged_keyword_counts_daily = article_keywords_clean_daily_df \
    .groupBy("ArticleID") \
    .agg(countDistinct("KeywordID").alias("TaggedKeywordCountInArticle"))

reference_source_counts_daily = article_references_clean_daily_df \
    .groupBy("ArticleID") \
    .agg(countDistinct("ReferenceID").alias("ReferenceSourceCountInArticle"))

articles_with_text_metrics_daily = articles_clean_df_daily \
    .withColumn("WordCountInMainContent", when(col("MainContent").isNotNull(), size(split(col("MainContent"), " "))).otherwise(0)) \
    .withColumn("CharacterCountInMainContent", when(col("MainContent").isNotNull(), length(col("MainContent"))).otherwise(0)) \
    .withColumn("EstimatedReadTimeMinutes", coalesce(_round(col("WordCountInMainContent") / 200.0, 2).cast(FloatType()), lit(0.0)))

fact_article_publication_source_daily = articles_with_text_metrics_daily \
    .join(tagged_keyword_counts_daily, articles_with_text_metrics_daily.ArticleID == tagged_keyword_counts_daily.ArticleID, "left_outer") \
    .join(reference_source_counts_daily, articles_with_text_metrics_daily.ArticleID == reference_source_counts_daily.ArticleID, "left_outer") \
    .select(
        articles_with_text_metrics_daily.ArticleID.alias("ArticleID_NK"),
        articles_with_text_metrics_daily.Title.alias("ArticleTitle"),
        articles_with_text_metrics_daily.Description.alias("ArticleDescription"),
        articles_with_text_metrics_daily.PublicationDate.alias("ArticlePublicationTimestamp"),
        articles_with_text_metrics_daily.OpinionCount,
        coalesce(articles_with_text_metrics_daily.WordCountInMainContent, lit(0)).alias("WordCountInMainContent"),
        coalesce(articles_with_text_metrics_daily.CharacterCountInMainContent, lit(0)).alias("CharacterCountInMainContent"),
        col("EstimatedReadTimeMinutes"),
        coalesce(tagged_keyword_counts_daily.TaggedKeywordCountInArticle, lit(0)).alias("TaggedKeywordCountInArticle"),
        coalesce(reference_source_counts_daily.ReferenceSourceCountInArticle, lit(0)).alias("ReferenceSourceCountInArticle"),
        articles_with_text_metrics_daily.AuthorID.alias("AuthorID_lookup"),
        articles_with_text_metrics_daily.TopicID.alias("TopicID_lookup"),
        articles_with_text_metrics_daily.SubTopicID.alias("SubTopicID_lookup")
    )

fact_article_publication_daily_df = fact_article_publication_source_daily \
    .withColumn("PublicationDateOnly", to_date(col("ArticlePublicationTimestamp"))) \
    .join(dim_date_loaded_df, col("PublicationDateOnly") == dim_date_loaded_df.FullDateAlternateKey, "left_outer") \
    .join(dim_author_loaded_df, col("AuthorID_lookup") == dim_author_loaded_df.AuthorID_NK, "left_outer") \
    .join(dim_topic_loaded_df, col("TopicID_lookup") == dim_topic_loaded_df.TopicID_NK, "left_outer") \
    .join(dim_subtopic_loaded_df, col("SubTopicID_lookup") == dim_subtopic_loaded_df.SubTopicID_NK, "left_outer") \
    .select(
        col("DateKey").alias("PublicationDateKey"),
        col("ArticlePublicationTimestamp"), # Cần cột này để Iceberg biết phân vùng nào cần overwrite
        col("AuthorKey"), col("TopicKey"), col("SubTopicKey"),
        col("ArticleID_NK"), col("ArticleTitle"), col("ArticleDescription"),
        lit(1).alias("PublishedArticleCount"), col("OpinionCount"),
        col("WordCountInMainContent"), col("CharacterCountInMainContent"),
        col("EstimatedReadTimeMinutes"), col("TaggedKeywordCountInArticle"),
        col("ReferenceSourceCountInArticle")
    ).na.fill({"PublicationDateKey": -1, "AuthorKey": -1, "TopicKey": -1, "SubTopicKey": -1})

if fact_article_publication_daily_df.count() > 0:
    write_curated_fact_table_daily(fact_article_publication_daily_df, "FactArticlePublication",
                                   mode="overwrite_partitions",
                                   partition_cols=["ArticlePublicationTimestamp"]) # Iceberg sẽ dùng transform days() từ DDL
else:
    print("Không có dữ liệu FactArticlePublication mới để ghi.")


# 3.2. FactArticleKeyword (DELETE rồi APPEND)
print("Đang xử lý FactArticleKeyword (DELETE/APPEND)...")
# fact_pub_keys_daily được lấy từ dữ liệu 7 ngày đã join với dimension
fact_pub_keys_daily = fact_article_publication_daily_df.select("ArticleID_NK", "PublicationDateKey", "AuthorKey", "TopicKey", "SubTopicKey") \
                    .withColumnRenamed("PublicationDateKey", "ArticlePublicationDateKey")

fact_article_keyword_daily_df = article_keywords_clean_daily_df \
    .join(fact_pub_keys_daily, article_keywords_clean_daily_df.ArticleID == fact_pub_keys_daily.ArticleID_NK, "inner") \
    .join(dim_keyword_loaded_df, article_keywords_clean_daily_df.KeywordID == dim_keyword_loaded_df.KeywordID_NK, "left_outer") \
    .select(
        col("ArticlePublicationDateKey"), col("ArticleID").alias("ArticleID_NK"),
        col("KeywordKey"), col("AuthorKey"), col("TopicKey"), col("SubTopicKey"),
        lit(1).alias("IsKeywordTaggedToArticle")
    ).na.fill(-1, subset=["ArticlePublicationDateKey", "KeywordKey", "AuthorKey", "TopicKey", "SubTopicKey"])

if fact_article_keyword_daily_df.count() > 0:
    write_curated_fact_table_daily(fact_article_keyword_daily_df, "FactArticleKeyword",
                                   mode="delete_append",
                                   delete_condition_col="ArticleID_NK",
                                   delete_values_df=article_ids_7_days_df) # Dùng ArticleID của 7 ngày để xóa
else:
    print("Không có dữ liệu FactArticleKeyword mới để ghi.")


# 3.3. FactArticleReference (DELETE rồi APPEND) - Tương tự FactArticleKeyword
print("Đang xử lý FactArticleReference (DELETE/APPEND)...")
fact_article_reference_daily_df = article_references_clean_daily_df \
    .join(fact_pub_keys_daily, article_references_clean_daily_df.ArticleID == fact_pub_keys_daily.ArticleID_NK, "inner") \
    .join(dim_referencesource_loaded_df, article_references_clean_daily_df.ReferenceID == dim_referencesource_loaded_df.ReferenceID_NK, "left_outer") \
    .select(
        col("ArticlePublicationDateKey"), col("ArticleID").alias("ArticleID_NK"),
        col("ReferenceSourceKey"), col("AuthorKey"), col("TopicKey"), col("SubTopicKey"),
        lit(1).alias("IsReferenceUsedInArticle")
    ).na.fill(-1, subset=["ArticlePublicationDateKey", "ReferenceSourceKey", "AuthorKey", "TopicKey", "SubTopicKey"])

if fact_article_reference_daily_df.count() > 0:
    write_curated_fact_table_daily(fact_article_reference_daily_df, "FactArticleReference",
                                   mode="delete_append",
                                   delete_condition_col="ArticleID_NK",
                                   delete_values_df=article_ids_7_days_df)
else:
    print("Không có dữ liệu FactArticleReference mới để ghi.")

# 3.4. FactTopCommentActivity (DELETE rồi APPEND)
print("Đang xử lý FactTopCommentActivity (DELETE/APPEND)...")
fact_top_comment_activity_daily_df = comments_clean_daily_df \
    .join(fact_pub_keys_daily, comments_clean_daily_df.ArticleID == fact_pub_keys_daily.ArticleID_NK, "inner") \
    .select(
        col("ArticlePublicationDateKey"),
        col("ArticlePublicationDateKey").alias("CommentDateKey"),
        col("ArticleID").alias("ArticleID_NK"),
        col("CommentID").alias("CommentID_NK"),
        col("AuthorKey"), col("TopicKey"), col("SubTopicKey"),
        col("CommenterName"),
        lit(1).alias("IsTopComment"),
        col("TotalLikes").alias("LikesOnTopComment")
    ).na.fill({"ArticlePublicationDateKey": -1, "CommentDateKey": -1, "AuthorKey": -1, "TopicKey": -1, "SubTopicKey": -1})

if fact_top_comment_activity_daily_df.count() > 0:
    write_curated_fact_table_daily(fact_top_comment_activity_daily_df, "FactTopCommentActivity",
                                   mode="delete_append",
                                   delete_condition_col="ArticleID_NK",
                                   delete_values_df=article_ids_7_days_df)
else:
    print("Không có dữ liệu FactTopCommentActivity mới để ghi.")


# 3.5. FactTopCommentInteractionDetail (DELETE rồi APPEND)
print("Đang xử lý FactTopCommentInteractionDetail (DELETE/APPEND)...")
# Lấy CommentID của 7 ngày để làm điều kiện xóa
comment_ids_7_days_for_interactions_df = fact_top_comment_activity_daily_df.select("CommentID_NK").distinct()

fact_top_comment_interaction_detail_daily_df = comment_interactions_clean_daily_df \
    .join(comments_clean_daily_df.select("CommentID", "ArticleID"), ["CommentID"], "inner") \
    .join(fact_pub_keys_daily, col("ArticleID") == fact_pub_keys_daily.ArticleID_NK, "inner") \
    .join(dim_interactiontype_loaded_df, comment_interactions_clean_daily_df.InteractionType == dim_interactiontype_loaded_df.InteractionTypeName, "left_outer") \
    .select(
        col("ArticlePublicationDateKey"),
        col("ArticlePublicationDateKey").alias("InteractionDateKey"),
        col("ArticleID").alias("ArticleID_NK"),
        col("CommentID").alias("CommentID_NK"),
        col("InteractionTypeKey"),
        col("AuthorKey"), col("TopicKey"), col("SubTopicKey"),
        lit(1).alias("InteractionInstanceCount"),
        col("InteractionCount").alias("InteractionValue")
    ).na.fill({"ArticlePublicationDateKey": -1, "InteractionDateKey": -1, "InteractionTypeKey": -1, "AuthorKey": -1, "TopicKey": -1, "SubTopicKey": -1})

if fact_top_comment_interaction_detail_daily_df.count() > 0:
     write_curated_fact_table_daily(fact_top_comment_interaction_detail_daily_df, "FactTopCommentInteractionDetail",
                                   mode="delete_append",
                                   delete_condition_col="CommentID_NK", # Xóa theo CommentID
                                   delete_values_df=comment_ids_7_days_for_interactions_df)
else:
    print("Không có dữ liệu FactTopCommentInteractionDetail mới để ghi.")


print("\n--- Hoàn thành quy trình ETL DAILY UPDATE từ Clean sang Curated ---")
spark.stop()
