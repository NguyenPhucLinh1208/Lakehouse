from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_timestamp, current_timestamp,
    lit, when, expr, sha2, concat_ws, coalesce, udf, from_json
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType, MapType
from datetime import datetime, timedelta
import os

# --- Cấu hình MinIO, Nessie ---
minio_endpoint = "http://minio1:9000"
minio_access_key = "rH4arFLYBxl55rh2zmN1"
minio_secret_key = "AtEB2XiMAznxvJXRvaXxigdIewIMVKscgPg7dJjI"
nessie_uri = "http://nessie:19120/api/v2"
nessie_default_branch = "main"

# --- Cấu hình Catalog và Database cho CLEAN zone ---
clean_catalog_name = "nessie-clean-news"
clean_catalog_warehouse_path = "s3a://clean-news-lakehouse/nessie_clean_news_warehouse"
CLEAN_DATABASE_NAME = "news_clean_db"

app_name = "NewsETLRawToClean_V2"

# --- CẤU HÌNH THỜI GIAN ETL ---
ETL_START_DATE_STR = "2025-05-08"
ETL_END_DATE_STR = "2025-05-15"
RAW_BASE_S3_PATH = "s3a://raw-news-lakehouse"


spark_builder = SparkSession.builder.appName(app_name)

# Cấu hình S3A và Spark SQL Extensions
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

print(f"SparkSession đã được khởi tạo cho ETL: {app_name}.")

# --- HÀM TIỆN ÍCH ---
def generate_surrogate_key(*cols_to_hash):
    return sha2(concat_ws("||", *cols_to_hash), 256)

def get_s3_paths_for_date_range(base_s3_path, start_date_str=None, end_date_str=None):
    paths = []
    date_format = "%Y-%m-%d"

    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, date_format)
            end_date = datetime.strptime(end_date_str, date_format)
            print(f"Sử dụng khoảng thời gian tùy chỉnh: {start_date_str} đến {end_date_str}")
        except ValueError:
            print(f"Lỗi định dạng ngày: {start_date_str}, {end_date_str}. Vui lòng dùng YYYY-MM-DD.")
            return []
    else:
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=6)
        print(f"Sử dụng khoảng thời gian mặc định: 7 ngày gần nhất, từ {start_date.strftime(date_format)} đến {end_date.strftime(date_format)}")

    current_date = start_date
    while current_date <= end_date:
        path = f"{base_s3_path}/{current_date.strftime('%Y/%m/%d')}/*.jsonl"
        paths.append(path)
        current_date += timedelta(days=1)
    return paths

# --- ĐỌC DỮ LIỆU RAW ---
raw_schema = StructType([
    StructField("title", StringType(), True), StructField("url", StringType(), True),
    StructField("description", StringType(), True), StructField("topic", StringType(), True),
    StructField("sub_topic", StringType(), True), StructField("tu_khoa", ArrayType(StringType()), True),
    StructField("ngay_xuat_ban", StringType(), True), StructField("tac_gia", StringType(), True),
    StructField("tham_khao", ArrayType(StringType()), True), StructField("noi_dung_chinh", StringType(), True),
    StructField("y_kien", StringType(), True), StructField("top_binh_luan", StringType(), True)
])
comment_interaction_detail_schema = MapType(StringType(), StringType())
top_binh_luan_item_schema = StructType([
    StructField("nguoi_binh_luan", StringType(), True), StructField("noi_dung", StringType(), True),
    StructField("tong_luot_thich", StringType(), True), StructField("chi_tiet_tuong_tac", comment_interaction_detail_schema, True)
])
top_binh_luan_array_schema = ArrayType(top_binh_luan_item_schema)

raw_data_paths = get_s3_paths_for_date_range(RAW_BASE_S3_PATH, ETL_START_DATE_STR, ETL_END_DATE_STR)

if not raw_data_paths:
    print("Không có đường dẫn dữ liệu nào được tạo cho khoảng thời gian đã chọn. Dừng job.")
    spark.stop()
    exit(0)

try:
    print(f"Đang đọc dữ liệu raw từ các đường dẫn: {raw_data_paths}")
    raw_df_initial = spark.read.schema(raw_schema).json(raw_data_paths)
    raw_df_initial = raw_df_initial.filter(col("url").isNotNull() & (col("url") != ""))
    count_raw_initial = raw_df_initial.count()
    print(f"Đã đọc dữ liệu raw thành công. Số lượng bản ghi raw ban đầu (có URL): {count_raw_initial}")
    if count_raw_initial == 0:
        print("Không có dữ liệu raw nào được đọc hoặc tất cả bản ghi không có URL. Dừng job.")
        spark.stop()
        exit(0)
except Exception as e:
    print(f"Lỗi khi đọc dữ liệu raw: {e}")
    spark.stop()
    exit(1)

raw_df_with_article_id = raw_df_initial.withColumn("ArticleID", generate_surrogate_key(col("url")))

print("--- BẮT ĐẦU QUÁ TRÌNH TRANSFORM ---")

# --- Xử lý bảng AUTHORS ---
print("\n--- Xử lý bảng AUTHORS ---")
authors_intermediate_df = raw_df_with_article_id.select(col("tac_gia").alias("AuthorName")) \
    .filter(col("AuthorName").isNotNull() & (col("AuthorName") != "")) \
    .distinct()
authors_df = authors_intermediate_df \
    .withColumn("AuthorID", generate_surrogate_key(col("AuthorName"))) \
    .select("AuthorID", "AuthorName") \
    .dropDuplicates(["AuthorID"])

# --- Xử lý bảng TOPICS ---
print("\n--- Xử lý bảng TOPICS ---")
topics_intermediate_df = raw_df_with_article_id.select(col("topic").alias("TopicName")) \
    .filter(col("TopicName").isNotNull() & (col("TopicName") != "")) \
    .distinct()
topics_df = topics_intermediate_df \
    .withColumn("TopicID", generate_surrogate_key(col("TopicName"))) \
    .select("TopicID", "TopicName") \
    .dropDuplicates(["TopicID"])

# --- Xử lý bảng SUBTOPICS ---
print("\n--- Xử lý bảng SUBTOPICS ---")
subtopics_intermediate_df = raw_df_with_article_id \
    .select(col("sub_topic").alias("SubTopicName"), col("topic").alias("ParentTopicName")) \
    .filter(col("SubTopicName").isNotNull() & (col("SubTopicName") != "") & \
            col("ParentTopicName").isNotNull() & (col("ParentTopicName") != "")) \
    .distinct()
subtopics_df = subtopics_intermediate_df \
    .join(topics_df, subtopics_intermediate_df.ParentTopicName == topics_df.TopicName, "inner") \
    .withColumn("SubTopicID", generate_surrogate_key(col("SubTopicName"), col("TopicID"))) \
    .select(col("SubTopicID"), col("SubTopicName"), col("TopicID")) \
    .dropDuplicates(["SubTopicID"])

# --- Xử lý bảng KEYWORDS (Dimension) ---
print("\n--- Xử lý bảng KEYWORDS (Dimension) ---")
keywords_dim_intermediate_df = raw_df_with_article_id \
    .filter(col("tu_khoa").isNotNull() & (expr("size(tu_khoa) > 0"))) \
    .select(explode(col("tu_khoa")).alias("KeywordText")) \
    .filter(col("KeywordText").isNotNull() & (col("KeywordText") != "")) \
    .distinct()
keywords_dim_df = keywords_dim_intermediate_df \
    .withColumn("KeywordID", generate_surrogate_key(col("KeywordText"))) \
    .select("KeywordID", "KeywordText") \
    .dropDuplicates(["KeywordID"])

# --- Xử lý bảng REFERENCES_TABLE (Dimension) ---
print("\n--- Xử lý bảng REFERENCES_TABLE (Dimension) ---")
references_dim_intermediate_df = raw_df_with_article_id \
    .filter(col("tham_khao").isNotNull() & (expr("size(tham_khao) > 0"))) \
    .select(explode(col("tham_khao")).alias("ReferenceText")) \
    .filter(col("ReferenceText").isNotNull() & (col("ReferenceText") != "")) \
    .distinct()
references_dim_df = references_dim_intermediate_df \
    .withColumn("ReferenceID", generate_surrogate_key(col("ReferenceText"))) \
    .select("ReferenceID", "ReferenceText") \
    .dropDuplicates(["ReferenceID"])

# --- Xử lý bảng ARTICLES ---
print("\n--- Xử lý bảng ARTICLES ---")
articles_base_transformed_df = raw_df_with_article_id \
    .withColumn("PublicationDate", to_timestamp(col("ngay_xuat_ban"))) \
    .withColumn("OpinionCount", coalesce(col("y_kien").cast(IntegerType()), lit(0)))

articles_base_aliased = articles_base_transformed_df.alias("base")
authors_aliased = authors_df.alias("auth")
topics_aliased = topics_df.alias("top")
subtopics_aliased = subtopics_df.alias("sub")

articles_joined_df = articles_base_aliased \
    .join(authors_aliased, col("base.tac_gia") == col("auth.AuthorName"), "left_outer") \
    .join(topics_aliased, col("base.topic") == col("top.TopicName"), "left_outer") \
    .join(subtopics_aliased,
          (col("base.sub_topic") == col("sub.SubTopicName")) & \
          (col("top.TopicID") == col("sub.TopicID")),
          "left_outer")

articles_to_write_df = articles_joined_df.select(
    col("base.ArticleID"),
    col("base.title").alias("Title"),
    col("base.url").alias("URL"),
    col("base.description").alias("Description"),
    col("base.PublicationDate"),
    col("base.noi_dung_chinh").alias("MainContent"),
    col("base.OpinionCount"),
    col("auth.AuthorID").alias("AuthorID"),
    col("top.TopicID").alias("TopicID"),
    col("sub.SubTopicID").alias("SubTopicID"),
    current_timestamp().alias("LastProcessedTimestamp")
).dropDuplicates(["ArticleID"])

# --- Xử lý bảng ARTICLEKEYWORDS (Junction) ---
print("\n--- Xử lý bảng ARTICLEKEYWORDS (Junction) ---")
article_keywords_exploded_df = raw_df_with_article_id \
    .filter(col("tu_khoa").isNotNull() & (expr("size(tu_khoa) > 0"))) \
    .select(col("ArticleID"), explode(col("tu_khoa")).alias("KeywordText")) \
    .filter(col("KeywordText").isNotNull() & (col("KeywordText") != ""))

article_keywords_final_df = article_keywords_exploded_df \
    .join(keywords_dim_df, article_keywords_exploded_df.KeywordText == keywords_dim_df.KeywordText, "inner") \
    .select("ArticleID", "KeywordID") \
    .distinct()

# --- Xử lý bảng ARTICLEREFERENCES (Junction) ---
print("\n--- Xử lý bảng ARTICLEREFERENCES (Junction) ---")
article_references_exploded_df = raw_df_with_article_id \
    .filter(col("tham_khao").isNotNull() & (expr("size(tham_khao) > 0"))) \
    .select(col("ArticleID"), explode(col("tham_khao")).alias("ReferenceText")) \
    .filter(col("ReferenceText").isNotNull() & (col("ReferenceText") != ""))

article_references_final_df = article_references_exploded_df \
    .join(references_dim_df, article_references_exploded_df.ReferenceText == references_dim_df.ReferenceText, "inner") \
    .select("ArticleID", "ReferenceID") \
    .distinct()

# --- Xử lý bảng COMMENTS và COMMENTINTERACTIONS ---
print("\n--- Xử lý bảng COMMENTS và COMMENTINTERACTIONS ---")
def parse_comments_udf_func(json_string):
    if json_string is None or "Không tìm thấy bình luận" in json_string or json_string.strip() == "":
        return None
    return json_string
parse_comments_spark_udf = udf(parse_comments_udf_func, StringType())

comments_parsed_base_df = raw_df_with_article_id \
    .select("ArticleID", "top_binh_luan") \
    .withColumn("parsed_comments_str", parse_comments_spark_udf(col("top_binh_luan"))) \
    .filter(col("parsed_comments_str").isNotNull())

if comments_parsed_base_df.count() > 0:
    comments_exploded_df = comments_parsed_base_df \
        .withColumn("parsed_comments_array", from_json(col("parsed_comments_str"), top_binh_luan_array_schema)) \
        .filter(col("parsed_comments_array").isNotNull()) \
        .select(col("ArticleID"), explode(col("parsed_comments_array")).alias("comment_data"))
else:
    comments_exploded_df = spark.createDataFrame([], StructType([ # Schema rỗng khớp với select mong muốn
        StructField("ArticleID", StringType(), True),
        StructField("comment_data", top_binh_luan_item_schema, True)
    ]))


if comments_exploded_df.count() == 0:
    print("Không có dữ liệu bình luận hợp lệ để xử lý.")
    comments_schema = StructType([
        StructField("CommentID", StringType(), True), StructField("ArticleID", StringType(), True),
        StructField("CommenterName", StringType(), True), StructField("CommentContent", StringType(), True),
        StructField("TotalLikes", IntegerType(), True), StructField("LastProcessedTimestamp", TimestampType(), True)
    ])
    comments_to_write_df = spark.createDataFrame([], comments_schema)
    comment_interactions_schema = StructType([
        StructField("CommentInteractionID", StringType(), True), StructField("CommentID", StringType(), True),
        StructField("InteractionType", StringType(), True), StructField("InteractionCount", IntegerType(), True),
        StructField("LastProcessedTimestamp", TimestampType(), True)
    ])
    comment_interactions_to_write_df = spark.createDataFrame([], comment_interactions_schema)
else:
    comments_intermediate_df = comments_exploded_df \
        .withColumn("CommenterName", col("comment_data.nguoi_binh_luan")) \
        .withColumn("CommentContent", col("comment_data.noi_dung")) \
        .withColumn("TotalLikes", coalesce(col("comment_data.tong_luot_thich").cast(IntegerType()), lit(0))) \
        .withColumn("interaction_details_map", col("comment_data.chi_tiet_tuong_tac")) \
        .filter(col("CommenterName").isNotNull() & col("CommentContent").isNotNull())
    comments_intermediate_df = comments_intermediate_df \
        .withColumn("CommentID", generate_surrogate_key(col("ArticleID"), col("CommenterName"), col("CommentContent")))
    comments_final_df = comments_intermediate_df \
        .select("CommentID", "ArticleID", "CommenterName", "CommentContent", "TotalLikes", "interaction_details_map") \
        .dropDuplicates(["CommentID"])
    comments_to_write_df = comments_final_df \
        .withColumn("LastProcessedTimestamp", current_timestamp()) \
        .drop("interaction_details_map")

    unique_comments_for_interactions = comments_final_df \
        .filter(col("interaction_details_map").isNotNull() & (expr("size(interaction_details_map) > 0"))) \
        .select("CommentID", "interaction_details_map")

    if unique_comments_for_interactions.count() == 0:
        print("Không có dữ liệu chi tiết tương tác bình luận.")
        comment_interactions_schema = StructType([
            StructField("CommentInteractionID", StringType(), True), StructField("CommentID", StringType(), True),
            StructField("InteractionType", StringType(), True), StructField("InteractionCount", IntegerType(), True),
            StructField("LastProcessedTimestamp", TimestampType(), True)
        ])
        comment_interactions_to_write_df = spark.createDataFrame([], comment_interactions_schema)
    else:
        comment_interactions_exploded_df = unique_comments_for_interactions \
            .select(col("CommentID"), explode(col("interaction_details_map")).alias("InteractionType", "InteractionCountStr"))
        comment_interactions_intermediate_df = comment_interactions_exploded_df \
            .withColumn("InteractionCount", coalesce(col("InteractionCountStr").cast(IntegerType()), lit(0)))
        comment_interactions_to_write_df = comment_interactions_intermediate_df \
            .withColumn("CommentInteractionID", generate_surrogate_key(col("CommentID"), col("InteractionType"))) \
            .select("CommentInteractionID", "CommentID", "InteractionType", "InteractionCount") \
            .withColumn("LastProcessedTimestamp", current_timestamp()) \
            .dropDuplicates(["CommentInteractionID"])


# --- HÀM GHI DỮ LIỆU SỬ DỤNG MERGE INTO ---
def write_iceberg_table_with_merge(df_to_write, table_name_in_db, primary_key_cols,
                                     db_name=CLEAN_DATABASE_NAME, catalog_name=clean_catalog_name,
                                     partition_cols=None, create_table_if_not_exists=True):
    full_table_name = f"`{catalog_name}`.`{db_name}`.`{table_name_in_db}`"
    temp_view_name = f"{table_name_in_db}_source_view_{datetime.now().strftime('%Y%m%d%H%M%S%f')}" #Tên view tạm thời duy nhất

    if df_to_write.count() == 0:
        print(f"Không có dữ liệu để ghi/merge vào bảng {full_table_name}. Bỏ qua.")
        return

    df_to_write.createOrReplaceTempView(temp_view_name)
    print(f"Đang chuẩn bị MERGE INTO bảng: {full_table_name} từ view {temp_view_name}")

    try:
        table_exists = spark.sql(f"SHOW TABLES IN `{catalog_name}`.`{db_name}` LIKE '{table_name_in_db}'").count() > 0

        if not table_exists:
            if create_table_if_not_exists:
                print(f"Bảng {full_table_name} chưa tồn tại. Sẽ tạo bảng và ghi dữ liệu.")
                writer = df_to_write.writeTo(full_table_name) \
                    .tableProperty("write.format.default", "parquet") \
                    .tableProperty("write.nessie.identifier-properties.enabled", "true") # Dùng cho Nessie tagging
                if partition_cols:
                    writer = writer.partitionedBy(*partition_cols)
                writer.create()
                print(f"Đã tạo và ghi dữ liệu vào bảng '{full_table_name}' thành công.")
                return
            else:
                raise RuntimeError(f"Bảng {full_table_name} không tồn tại và create_table_if_not_exists=False.")

        merge_condition_parts = [f"target.{pk_col} = source.{pk_col}" for pk_col in primary_key_cols]
        merge_condition = " AND ".join(merge_condition_parts)

        merge_sql = f"""
        MERGE INTO {full_table_name} AS target
        USING {temp_view_name} AS source
        ON {merge_condition}
        WHEN MATCHED THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *
        """
        print(f"Thực thi MERGE SQL cho {full_table_name}: \n{merge_sql}")
        spark.sql(merge_sql)
        print(f"MERGE INTO bảng '{full_table_name}' thành công.")

    except Exception as e:
        print(f"Lỗi khi ghi/MERGE INTO bảng '{full_table_name}': {e}")
        raise
    finally:
        spark.catalog.dropTempView(temp_view_name)
        print(f"Đã xóa temp view: {temp_view_name}")


# --- GHI DỮ LIỆU VÀO CÁC BẢNG CLEAN ---
print("\n--- Bắt đầu ghi dữ liệu vào các bảng CLEAN sử dụng MERGE ---")

write_iceberg_table_with_merge(authors_df, "authors", primary_key_cols=["AuthorID"])
write_iceberg_table_with_merge(topics_df, "topics", primary_key_cols=["TopicID"])
write_iceberg_table_with_merge(subtopics_df, "subtopics", primary_key_cols=["SubTopicID"])
write_iceberg_table_with_merge(keywords_dim_df, "keywords", primary_key_cols=["KeywordID"])
write_iceberg_table_with_merge(references_dim_df, "references_table", primary_key_cols=["ReferenceID"])

write_iceberg_table_with_merge(articles_to_write_df, "articles",
                               primary_key_cols=["ArticleID"],
                               partition_cols=["PublicationDate"]) # PublicationDate là cột partition

write_iceberg_table_with_merge(article_keywords_final_df, "article_keywords",
                               primary_key_cols=["ArticleID", "KeywordID"])
write_iceberg_table_with_merge(article_references_final_df, "article_references",
                               primary_key_cols=["ArticleID", "ReferenceID"])

if 'comments_to_write_df' in locals(): # Kiểm tra biến tồn tại
    write_iceberg_table_with_merge(comments_to_write_df, "comments", primary_key_cols=["CommentID"])
else:
    print("Không có DataFrame comments_to_write_df để ghi.")

if 'comment_interactions_to_write_df' in locals(): # Kiểm tra biến tồn tại
    write_iceberg_table_with_merge(comment_interactions_to_write_df, "comment_interactions",
                                     primary_key_cols=["CommentInteractionID"])
else:
    print("Không có DataFrame comment_interactions_to_write_df để ghi.")

print("\n--- Hoàn thành quy trình ETL từ Raw sang Clean (V2) ---")
spark.stop()
