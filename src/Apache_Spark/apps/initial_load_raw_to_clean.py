from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, monotonically_increasing_id, to_timestamp, current_timestamp,
    lit, when, expr, sha2, concat_ws, coalesce, udf, from_json
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType, MapType

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

app_name = "InitialLoadRawToCleanNewsETL"

spark_builder = SparkSession.builder.appName(app_name)

# Cấu hình S3A và Spark SQL Extensions (giống như script trước)
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

print("SparkSession đã được khởi tạo cho Initial Load: Raw -> Clean.")

# --- ĐỌC DỮ LIỆU RAW LỊCH SỬ ---
# !!! QUAN TRỌNG: Điều chỉnh raw_data_path để đọc TOÀN BỘ dữ liệu lịch sử !!!
# Ví dụ: đọc tất cả các file .jsonl trong một thư mục lớn hoặc nhiều thư mục con
raw_data_path = "s3a://raw-news-lakehouse/2024/*/*/*.jsonl" # Ví dụ: bao gồm các thư mục con
# Hoặc bạn có thể truyền danh sách các đường dẫn nếu dữ liệu nằm rải rác
# raw_data_path = [
# "s3a://raw-news-lakehouse/2020/**/*.jsonl",
# "s3a://raw-news-lakehouse/2021/**/*.jsonl",
# # ... đến ngày hiện tại trước lần chạy đầu tiên
# ]

# Schemas (Không đổi)
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

try:
    print(f"Đang đọc dữ liệu raw lịch sử từ: {raw_data_path}")
    raw_df = spark.read.schema(raw_schema).json(raw_data_path)
    print("Đã đọc dữ liệu raw lịch sử thành công.")
    print(f"Số lượng bản ghi raw lịch sử: {raw_df.count()}")
except Exception as e:
    print(f"Lỗi khi đọc dữ liệu raw lịch sử: {e}")
    spark.stop()
    exit(1)

raw_df = raw_df.withColumn("RawRecordUID", monotonically_increasing_id())

def generate_surrogate_key(*cols_to_hash):
    return sha2(concat_ws("||", *cols_to_hash), 256)

# --- PHẦN XỬ LÝ DỮ LIỆU (authors_df đến articles_to_write_df) GIỮ NGUYÊN ---
# Logic transform để tạo các DataFrame trung gian là giống nhau cho cả initial và daily load
# (Sao chép toàn bộ phần xử lý từ script trước của bạn vào đây)
# Bắt đầu từ: print("\n--- Xử lý bảng AUTHORS ---")
# ...
# Kết thúc ở: articles_to_write_df = articles_final_df.drop("RawRecordUID")
# và phần xử lý comments, comment_interactions
# (Để tránh lặp lại quá nhiều code, tôi giả sử bạn sẽ copy phần đó vào đây)

# VÍ DỤ: (Copy toàn bộ phần transform từ script trước)
print("\n--- Xử lý bảng AUTHORS ---")
authors_intermediate_df = raw_df.select(col("tac_gia").alias("AuthorName")) \
    .filter(col("AuthorName").isNotNull() & (col("AuthorName") != "")) \
    .distinct()
authors_df = authors_intermediate_df \
    .withColumn("AuthorID", generate_surrogate_key(col("AuthorName"))) \
    .select("AuthorID", "AuthorName")

print("\n--- Xử lý bảng TOPICS ---")
topics_intermediate_df = raw_df.select(col("topic").alias("TopicName")) \
    .filter(col("TopicName").isNotNull() & (col("TopicName") != "")) \
    .distinct()
topics_df = topics_intermediate_df \
    .withColumn("TopicID", generate_surrogate_key(col("TopicName"))) \
    .select("TopicID", "TopicName")

print("\n--- Xử lý bảng SUBTOPICS ---")
subtopics_intermediate_df = raw_df \
    .select(col("sub_topic").alias("SubTopicName"), col("topic").alias("ParentTopicName")) \
    .filter(col("SubTopicName").isNotNull() & (col("SubTopicName") != "") & \
            col("ParentTopicName").isNotNull() & (col("ParentTopicName") != "")) \
    .distinct()
subtopics_df = subtopics_intermediate_df \
    .join(topics_df, subtopics_intermediate_df.ParentTopicName == topics_df.TopicName, "inner") \
    .withColumn("SubTopicID", generate_surrogate_key(col("SubTopicName"), col("TopicID"))) \
    .select(col("SubTopicID"), col("SubTopicName"), col("TopicID"))

print("\n--- Xử lý bảng KEYWORDS và ARTICLEKEYWORDS ---")
keywords_exploded_df = raw_df \
    .filter(col("tu_khoa").isNotNull() & (expr("size(tu_khoa) > 0"))) \
    .select(col("RawRecordUID"), explode(col("tu_khoa")).alias("KeywordText")) \
    .filter(col("KeywordText").isNotNull() & (col("KeywordText") != ""))
keywords_dim_df = keywords_exploded_df.select("KeywordText").distinct() \
    .withColumn("KeywordID", generate_surrogate_key(col("KeywordText"))) \
    .select("KeywordID", "KeywordText")
article_keywords_junction_df = keywords_exploded_df \
    .join(keywords_dim_df, keywords_exploded_df.KeywordText == keywords_dim_df.KeywordText, "inner") \
    .select(col("RawRecordUID"), col("KeywordID"))

print("\n--- Xử lý bảng REFERENCES_TABLE và ARTICLEREFERENCES ---")
references_exploded_df = raw_df \
    .filter(col("tham_khao").isNotNull() & (expr("size(tham_khao) > 0"))) \
    .select(col("RawRecordUID"), explode(col("tham_khao")).alias("ReferenceText")) \
    .filter(col("ReferenceText").isNotNull() & (col("ReferenceText") != ""))
references_dim_df = references_exploded_df.select("ReferenceText").distinct() \
    .withColumn("ReferenceID", generate_surrogate_key(col("ReferenceText"))) \
    .select("ReferenceID", "ReferenceText")
article_references_junction_df = references_exploded_df \
    .join(references_dim_df, references_exploded_df.ReferenceText == references_dim_df.ReferenceText, "inner") \
    .select(col("RawRecordUID"), col("ReferenceID"))

print("\n--- Xử lý bảng ARTICLES ---")
articles_base_transformed_df = raw_df \
    .withColumn("PublicationDate", to_timestamp(col("ngay_xuat_ban"))) \
    .withColumn("OpinionCount", coalesce(col("y_kien").cast(IntegerType()), lit(0)))
articles_base_transformed_df = articles_base_transformed_df \
    .withColumn("ArticleID", generate_surrogate_key(col("url")))
articles_base_aliased = articles_base_transformed_df.alias("base")
authors_aliased = authors_df.alias("auth")
topics_aliased = topics_df.alias("top")
subtopics_aliased = subtopics_df.alias("sub")
articles_joined_df = articles_base_aliased \
    .join(authors_aliased, col("base.tac_gia") == col("auth.AuthorName"), "left_outer") \
    .join(topics_aliased, col("base.topic") == col("top.TopicName"), "left_outer") \
    .join(subtopics_aliased, (col("base.sub_topic") == col("sub.SubTopicName")) & \
                             (col("top.TopicID") == col("sub.TopicID")), "left_outer")
articles_final_df = articles_joined_df.select(
    col("base.ArticleID"), col("base.title").alias("Title"),
    col("base.url").alias("URL"), col("base.description").alias("Description"),
    col("base.PublicationDate"), col("base.noi_dung_chinh").alias("MainContent"),
    col("base.OpinionCount"), col("auth.AuthorID").alias("AuthorID"),
    col("top.TopicID").alias("TopicID"), col("sub.SubTopicID").alias("SubTopicID"),
    col("base.RawRecordUID")
).distinct()
article_keywords_final_df = article_keywords_junction_df \
    .join(articles_final_df.select("RawRecordUID", "ArticleID"), ["RawRecordUID"], "inner") \
    .select("ArticleID", "KeywordID") \
    .distinct()
article_references_final_df = article_references_junction_df \
    .join(articles_final_df.select("RawRecordUID", "ArticleID"), ["RawRecordUID"], "inner") \
    .select("ArticleID", "ReferenceID") \
    .distinct()
articles_to_write_df = articles_final_df.drop("RawRecordUID")

print("\n--- Xử lý bảng COMMENTS và COMMENTINTERACTIONS ---")
def parse_comments_udf(json_string):
    if json_string is None or "Không tìm thấy bình luận" in json_string:
        return None
    return json_string
parse_comments_udf_spark = udf(parse_comments_udf, StringType())
comments_parsed_df = raw_df \
    .withColumn("parsed_comments_str", parse_comments_udf_spark(col("top_binh_luan"))) \
    .filter(col("parsed_comments_str").isNotNull()) \
    .withColumn("parsed_comments_array", from_json(col("parsed_comments_str"), top_binh_luan_array_schema)) \
    .filter(col("parsed_comments_array").isNotNull()) \
    .select(col("RawRecordUID"), explode(col("parsed_comments_array")).alias("comment_data"))

if comments_parsed_df.count() == 0:
    print("Không có dữ liệu bình luận hợp lệ để xử lý (Initial Load).")
    comments_schema = StructType([
        StructField("CommentID", StringType(), True), StructField("ArticleID", StringType(), True),
        StructField("CommenterName", StringType(), True), StructField("CommentContent", StringType(), True),
        StructField("TotalLikes", IntegerType(), True)
    ])
    comments_to_write_df = spark.createDataFrame([], comments_schema)
    comment_interactions_schema = StructType([
        StructField("CommentInteractionID", StringType(), True), StructField("CommentID", StringType(), True),
        StructField("InteractionType", StringType(), True), StructField("InteractionCount", IntegerType(), True)
    ])
    comment_interactions_to_write_df = spark.createDataFrame([], comment_interactions_schema)
else:
    comments_intermediate_df = comments_parsed_df \
        .withColumn("CommenterName", col("comment_data.nguoi_binh_luan")) \
        .withColumn("CommentContent", col("comment_data.noi_dung")) \
        .withColumn("TotalLikes", coalesce(col("comment_data.tong_luot_thich").cast(IntegerType()), lit(0))) \
        .withColumn("interaction_details_map", col("comment_data.chi_tiet_tuong_tac"))
    comments_intermediate_df = comments_intermediate_df \
        .withColumn("CommentID", generate_surrogate_key(col("RawRecordUID"), col("CommenterName"), col("CommentContent")))
    comments_final_df = comments_intermediate_df \
        .join(articles_final_df.select("RawRecordUID", "ArticleID"), ["RawRecordUID"], "inner") \
        .select("CommentID", "ArticleID", "CommenterName", "CommentContent", "TotalLikes", "interaction_details_map")
    comments_final_df = comments_final_df.dropDuplicates(["CommentID"])
    comments_to_write_df = comments_final_df.drop("interaction_details_map")
    unique_comments_for_interactions = comments_final_df.select("CommentID", "interaction_details_map")
    comment_interactions_exploded_df = unique_comments_for_interactions \
        .filter(col("interaction_details_map").isNotNull() & (expr("size(interaction_details_map) > 0"))) \
        .select(col("CommentID"), explode(col("interaction_details_map")).alias("InteractionType", "InteractionCountStr"))
    if comment_interactions_exploded_df.count() == 0:
        print("Không có dữ liệu chi tiết tương tác bình luận (Initial Load).")
        comment_interactions_schema = StructType([
            StructField("CommentInteractionID", StringType(), True), StructField("CommentID", StringType(), True),
            StructField("InteractionType", StringType(), True), StructField("InteractionCount", IntegerType(), True)
        ])
        comment_interactions_to_write_df = spark.createDataFrame([], comment_interactions_schema)
    else:
        comment_interactions_intermediate_df = comment_interactions_exploded_df \
            .withColumn("InteractionCount", coalesce(col("InteractionCountStr").cast(IntegerType()), lit(0)))
        comment_interactions_to_write_df = comment_interactions_intermediate_df \
            .withColumn("CommentInteractionID", generate_surrogate_key(col("CommentID"), col("InteractionType"))) \
            .select("CommentInteractionID", "CommentID", "InteractionType", "InteractionCount")
        comment_interactions_to_write_df = comment_interactions_to_write_df.dropDuplicates(["CommentInteractionID"])

# --- HÀM GHI DỮ LIỆU ---
def write_iceberg_table_initial_load(df_to_write, table_name_in_db, db_name=CLEAN_DATABASE_NAME, catalog_name=clean_catalog_name, mode="overwrite", partition_cols=None):
    full_table_name = f"`{catalog_name}`.`{db_name}`.`{table_name_in_db}`"
    print(f"Đang ghi (Initial Load) vào bảng: {full_table_name} với mode '{mode}'")
    try:
        writer = df_to_write.writeTo(full_table_name) \
            .tableProperty("write.format.default", "parquet")

        if partition_cols:
            writer = writer.partitionedBy(*partition_cols)

        if mode == "overwrite": # Cho Initial Load, các bảng dimension-like và bảng nối/comment sẽ dùng overwrite
            writer.createOrReplace()
        elif mode == "append": # Cho articles
            writer.append()
        else:
            raise ValueError(f"Chế độ ghi '{mode}' không được hỗ trợ cho Initial Load trong hàm này.")
        print(f"Đã ghi dữ liệu vào bảng '{full_table_name}' thành công.")
    except Exception as e:
        print(f"Lỗi khi ghi vào bảng '{full_table_name}': {e}")
        raise

# --- Ghi dữ liệu vào các bảng CLEAN cho INITIAL LOAD ---
print("\n--- Bắt đầu ghi dữ liệu INITIAL LOAD vào các bảng CLEAN ---")

write_iceberg_table_initial_load(authors_df, "authors", mode="overwrite")
write_iceberg_table_initial_load(topics_df, "topics", mode="overwrite")
write_iceberg_table_initial_load(subtopics_df, "subtopics", mode="overwrite")
write_iceberg_table_initial_load(keywords_dim_df, "keywords", mode="overwrite")
write_iceberg_table_initial_load(references_dim_df, "references_table", mode="overwrite")

# articles sẽ append vì chúng ta nạp nhiều phân vùng lịch sử
write_iceberg_table_initial_load(articles_to_write_df, "articles", mode="append", partition_cols=["PublicationDate"])

# Bảng nối và comment cho initial load, có thể overwrite để đảm bảo sạch sẽ nếu chạy lại
write_iceberg_table_initial_load(article_keywords_final_df, "article_keywords", mode="overwrite")
write_iceberg_table_initial_load(article_references_final_df, "article_references", mode="overwrite")

if 'comments_to_write_df' in locals() and comments_to_write_df.count() > 0 :
    write_iceberg_table_initial_load(comments_to_write_df, "comments", mode="overwrite")
else:
    print("Không có dữ liệu comments để ghi (Initial Load).")

if 'comment_interactions_to_write_df' in locals() and comment_interactions_to_write_df.count() > 0:
    write_iceberg_table_initial_load(comment_interactions_to_write_df, "comment_interactions", mode="overwrite")
else:
    print("Không có dữ liệu comment_interactions để ghi (Initial Load).")

print("\n--- Hoàn thành quy trình ETL INITIAL LOAD từ Raw sang Clean ---")
spark.stop()
