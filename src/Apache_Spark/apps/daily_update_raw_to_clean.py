from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, monotonically_increasing_id, to_timestamp, current_timestamp,
    lit, when, expr, sha2, concat_ws, coalesce, udf, from_json,
    date_sub, current_date # Thêm hàm xử lý ngày
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType, MapType
from datetime import datetime, timedelta # Để tính toán khoảng ngày

# --- Cấu hình MinIO, Nessie (Tương tự) ---
minio_endpoint = "http://minio1:9000"
minio_access_key = "rH4arFLYBxl55rh2zmN1"
minio_secret_key = "AtEB2XiMAznxvJXRvaXxigdIewIMVKscgPg7dJjI"
nessie_uri = "http://nessie:19120/api/v2"
nessie_default_branch = "main"

# --- Cấu hình Catalog và Database cho CLEAN zone (Tương tự) ---
clean_catalog_name = "nessie-clean-news"
clean_catalog_warehouse_path = "s3a://clean-news-lakehouse/nessie_clean_news_warehouse"
CLEAN_DATABASE_NAME = "news_clean_db"

app_name = "DailyUpdateRawToCleanNewsETL"

spark_builder = SparkSession.builder.appName(app_name)
# Cấu hình SparkSession (Tương tự script initial load)
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

print("SparkSession đã được khởi tạo cho Daily Update: Raw -> Clean.")

# --- XÁC ĐỊNH PHẠM VI DỮ LIỆU 7 NGÀY GẦN NHẤT ---
# Giả sử chạy vào ngày D, lấy dữ liệu từ D-6 đến D
today_str = spark.sql("SELECT current_date()").first()[0].strftime('%Y-%m-%d')
# Hoặc bạn có thể truyền ngày vào làm tham số cho script
# today_str = "2025-05-24" # Ví dụ

date_list_7_days = []
for i in range(7):
    # target_date = spark.sql(f"SELECT date_sub(to_date('{today_str}'), {i})").first()[0]
    # date_list_7_days.append(target_date.strftime('%Y/%m/%d')) # Giả sử path là YYYY/MM/DD
    # Hoặc nếu cấu trúc path của bạn khác, ví dụ: s3a://raw-news-lakehouse/YYYY-MM-DD/topic.jsonl
    # thì cần tạo path cho từng ngày
    # Dưới đây là ví dụ đơn giản hóa, bạn cần xây dựng raw_data_paths_7_days
    # dựa trên cấu trúc thư mục raw data của bạn
    # Giả sử bạn có một hàm get_raw_path_for_date(date_obj)
    dt = datetime.strptime(today_str, '%Y-%m-%d') - timedelta(days=i)
    # Đây là ví dụ, bạn cần điều chỉnh cho đúng cấu trúc path của mình
    # Ví dụ: s3a://raw-news-lakehouse/YEAR/MONTH/DAY/FILE.jsonl
    # Hoặc s3a://raw-news-lakehouse/YYYY-MM-DD_topic.jsonl
    # Giả sử path là: s3a://raw-news-lakehouse/YYYY/MM/DD/*.jsonl
    date_list_7_days.append(f"s3a://raw-news-lakehouse/{dt.strftime('%Y/%m/%d')}/*.jsonl")


print(f"Sẽ xử lý dữ liệu raw cho 7 ngày, ví dụ các paths: {date_list_7_days[:2]}...") # In ra vài path ví dụ

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
    # Đọc dữ liệu raw cho 7 ngày. Nếu không có file nào trong paths, raw_df sẽ rỗng.
    raw_df = spark.read.schema(raw_schema).json(date_list_7_days)
    print(f"Đã đọc dữ liệu raw cho 7 ngày. Số lượng bản ghi: {raw_df.count()}")
    if raw_df.count() == 0:
        print("Không có dữ liệu raw mới trong 7 ngày qua. Kết thúc job.")
        spark.stop()
        exit(0)
except Exception as e:
    print(f"Lỗi khi đọc dữ liệu raw 7 ngày: {e}")
    spark.stop()
    exit(1)

raw_df = raw_df.withColumn("RawRecordUID", monotonically_increasing_id())

def generate_surrogate_key(*cols_to_hash):
    return sha2(concat_ws("||", *cols_to_hash), 256)

# --- PHẦN XỬ LÝ DỮ LIỆU (authors_df đến articles_to_write_df) GIỮ NGUYÊN ---
# Logic transform là giống nhau
# (Sao chép toàn bộ phần xử lý từ script initial load vào đây)
# ...
# VÍ DỤ: (Copy toàn bộ phần transform từ script initial load)
print("\n--- Xử lý bảng AUTHORS ---")
authors_intermediate_df = raw_df.select(col("tac_gia").alias("AuthorName")) \
    .filter(col("AuthorName").isNotNull() & (col("AuthorName") != "")) \
    .distinct()
authors_df_new = authors_intermediate_df \
    .withColumn("AuthorID", generate_surrogate_key(col("AuthorName"))) \
    .select("AuthorID", "AuthorName") # Đặt tên là _new để phân biệt với bảng clean hiện có

print("\n--- Xử lý bảng TOPICS ---")
topics_intermediate_df = raw_df.select(col("topic").alias("TopicName")) \
    .filter(col("TopicName").isNotNull() & (col("TopicName") != "")) \
    .distinct()
topics_df_new = topics_intermediate_df \
    .withColumn("TopicID", generate_surrogate_key(col("TopicName"))) \
    .select("TopicID", "TopicName")

print("\n--- Xử lý bảng SUBTOPICS ---")
subtopics_intermediate_df = raw_df \
    .select(col("sub_topic").alias("SubTopicName"), col("topic").alias("ParentTopicName")) \
    .filter(col("SubTopicName").isNotNull() & (col("SubTopicName") != "") & \
            col("ParentTopicName").isNotNull() & (col("ParentTopicName") != "")) \
    .distinct()
# Khi merge subtopics, cần TopicID từ topics_df_new hoặc bảng topics đã tồn tại
# Để đơn giản, giả sử topics_df_new chứa đủ hoặc chúng ta merge topics trước
# Trong thực tế, bạn cần đọc topics_clean_df hiện tại, merge với topics_df_new, rồi dùng kết quả để join
topics_for_subtopic_join = topics_df_new # Hoặc logic merge phức tạp hơn
subtopics_df_new = subtopics_intermediate_df \
    .join(topics_for_subtopic_join, subtopics_intermediate_df.ParentTopicName == topics_for_subtopic_join.TopicName, "inner") \
    .withColumn("SubTopicID", generate_surrogate_key(col("SubTopicName"), col("TopicID"))) \
    .select(col("SubTopicID"), col("SubTopicName"), col("TopicID"))


print("\n--- Xử lý bảng KEYWORDS và ARTICLEKEYWORDS ---")
keywords_exploded_df = raw_df \
    .filter(col("tu_khoa").isNotNull() & (expr("size(tu_khoa) > 0"))) \
    .select(col("RawRecordUID"), explode(col("tu_khoa")).alias("KeywordText")) \
    .filter(col("KeywordText").isNotNull() & (col("KeywordText") != ""))
keywords_dim_df_new = keywords_exploded_df.select("KeywordText").distinct() \
    .withColumn("KeywordID", generate_surrogate_key(col("KeywordText"))) \
    .select("KeywordID", "KeywordText")
article_keywords_junction_df = keywords_exploded_df \
    .join(keywords_dim_df_new, keywords_exploded_df.KeywordText == keywords_dim_df_new.KeywordText, "inner") \
    .select(col("RawRecordUID"), col("KeywordID"))

print("\n--- Xử lý bảng REFERENCES_TABLE và ARTICLEREFERENCES ---")
references_exploded_df = raw_df \
    .filter(col("tham_khao").isNotNull() & (expr("size(tham_khao) > 0"))) \
    .select(col("RawRecordUID"), explode(col("tham_khao")).alias("ReferenceText")) \
    .filter(col("ReferenceText").isNotNull() & (col("ReferenceText") != ""))
references_dim_df_new = references_exploded_df.select("ReferenceText").distinct() \
    .withColumn("ReferenceID", generate_surrogate_key(col("ReferenceText"))) \
    .select("ReferenceID", "ReferenceText")
article_references_junction_df = references_exploded_df \
    .join(references_dim_df_new, references_exploded_df.ReferenceText == references_dim_df_new.ReferenceText, "inner") \
    .select(col("RawRecordUID"), col("ReferenceID"))

print("\n--- Xử lý bảng ARTICLES ---")
articles_base_transformed_df = raw_df \
    .withColumn("PublicationDate", to_timestamp(col("ngay_xuat_ban"))) \
    .withColumn("OpinionCount", coalesce(col("y_kien").cast(IntegerType()), lit(0)))
articles_base_transformed_df = articles_base_transformed_df \
    .withColumn("ArticleID", generate_surrogate_key(col("url")))
articles_base_aliased = articles_base_transformed_df.alias("base")
# Khi join, nên join với _new DFs hoặc bảng clean hiện tại rồi merge
authors_aliased = authors_df_new.alias("auth") # Giả sử dùng _new cho đơn giản, thực tế cần merge
topics_aliased = topics_df_new.alias("top")
subtopics_aliased = subtopics_df_new.alias("sub")

articles_joined_df = articles_base_aliased \
    .join(authors_aliased, col("base.tac_gia") == col("auth.AuthorName"), "left_outer") \
    .join(topics_aliased, col("base.topic") == col("top.TopicName"), "left_outer") \
    .join(subtopics_aliased, (col("base.sub_topic") == col("sub.SubTopicName")) & \
                             (col("top.TopicID") == col("sub.TopicID")), "left_outer") # Cẩn thận join key ở đây
articles_final_df = articles_joined_df.select(
    col("base.ArticleID"), col("base.title").alias("Title"),
    col("base.url").alias("URL"), col("base.description").alias("Description"),
    col("base.PublicationDate"), col("base.noi_dung_chinh").alias("MainContent"),
    col("base.OpinionCount"), col("auth.AuthorID").alias("AuthorID"),
    col("top.TopicID").alias("TopicID"), col("sub.SubTopicID").alias("SubTopicID"),
    col("base.RawRecordUID")
).distinct() # distinct() có thể tốn kém, cân nhắc nếu ArticleID đã duy nhất
articles_to_write_df_daily = articles_final_df.drop("RawRecordUID") # Dữ liệu 7 ngày

# Cập nhật các bảng nối với ArticleID thực sự
article_keywords_final_df_daily = article_keywords_junction_df \
    .join(articles_final_df.select("RawRecordUID", "ArticleID"), ["RawRecordUID"], "inner") \
    .select("ArticleID", "KeywordID") \
    .distinct()
article_references_final_df_daily = article_references_junction_df \
    .join(articles_final_df.select("RawRecordUID", "ArticleID"), ["RawRecordUID"], "inner") \
    .select("ArticleID", "ReferenceID") \
    .distinct()

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
    print("Không có dữ liệu bình luận hợp lệ để xử lý (Daily Load).")
    # Tạo DF rỗng để logic delete/append không lỗi
    comments_schema = StructType([
        StructField("CommentID", StringType(), True), StructField("ArticleID", StringType(), True),
        StructField("CommenterName", StringType(), True), StructField("CommentContent", StringType(), True),
        StructField("TotalLikes", IntegerType(), True)
    ])
    comments_to_write_df_daily = spark.createDataFrame([], comments_schema)
    comment_interactions_schema = StructType([
        StructField("CommentInteractionID", StringType(), True), StructField("CommentID", StringType(), True),
        StructField("InteractionType", StringType(), True), StructField("InteractionCount", IntegerType(), True)
    ])
    comment_interactions_to_write_df_daily = spark.createDataFrame([], comment_interactions_schema)
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
    comments_to_write_df_daily = comments_final_df.drop("interaction_details_map")
    unique_comments_for_interactions = comments_final_df.select("CommentID", "interaction_details_map")
    comment_interactions_exploded_df = unique_comments_for_interactions \
        .filter(col("interaction_details_map").isNotNull() & (expr("size(interaction_details_map) > 0"))) \
        .select(col("CommentID"), explode(col("interaction_details_map")).alias("InteractionType", "InteractionCountStr"))
    if comment_interactions_exploded_df.count() == 0:
        print("Không có dữ liệu chi tiết tương tác bình luận (Daily Load).")
        comment_interactions_schema = StructType([
            StructField("CommentInteractionID", StringType(), True), StructField("CommentID", StringType(), True),
            StructField("InteractionType", StringType(), True), StructField("InteractionCount", IntegerType(), True)
        ])
        comment_interactions_to_write_df_daily = spark.createDataFrame([], comment_interactions_schema)
    else:
        comment_interactions_intermediate_df = comment_interactions_exploded_df \
            .withColumn("InteractionCount", coalesce(col("InteractionCountStr").cast(IntegerType()), lit(0)))
        comment_interactions_to_write_df_daily = comment_interactions_intermediate_df \
            .withColumn("CommentInteractionID", generate_surrogate_key(col("CommentID"), col("InteractionType"))) \
            .select("CommentInteractionID", "CommentID", "InteractionType", "InteractionCount")
        comment_interactions_to_write_df_daily = comment_interactions_to_write_df_daily.dropDuplicates(["CommentInteractionID"])


# --- HÀM GHI DỮ LIỆU VÀ MERGE CHO DAILY UPDATE ---
def merge_into_iceberg_table(df_new_data, table_name_in_db, pk_column_name, db_name=CLEAN_DATABASE_NAME, catalog_name=clean_catalog_name):
    """Thực hiện MERGE INTO cho các bảng dimension-like."""
    full_table_name = f"`{catalog_name}`.`{db_name}`.`{table_name_in_db}`"
    df_new_data.createOrReplaceTempView(f"new_{table_name_in_db}")
    
    # Giả sử các bảng dimension-like này chỉ có 2 cột: ID và Name/Text
    # Bạn cần điều chỉnh mệnh đề UPDATE SET cho phù hợp với số lượng cột thực tế
    update_set_clause = ", ".join([f"target.{col_name} = source.{col_name}" for col_name in df_new_data.columns if col_name != pk_column_name])
    insert_columns = ", ".join(df_new_data.columns)
    insert_values = ", ".join([f"source.{col_name}" for col_name in df_new_data.columns])

    merge_sql = f"""
    MERGE INTO {full_table_name} target
    USING new_{table_name_in_db} source
    ON target.{pk_column_name} = source.{pk_column_name}
    WHEN MATCHED AND ({update_set_clause.replace('target.', 'target.').replace('source.', 'NOT(target.').replace(' = ', ' <=> source.')}) -- Kiểm tra nếu có gì đó khác biệt
    THEN UPDATE SET {update_set_clause}
    WHEN NOT MATCHED
    THEN INSERT ({insert_columns}) VALUES ({insert_values})
    """
    # Lưu ý: Điều kiện WHEN MATCHED AND (...) để chỉ update khi có thay đổi thực sự là phức tạp.
    # Để đơn giản, có thể chỉ cần WHEN MATCHED THEN UPDATE SET ...
    # Hoặc nếu bảng chỉ có 2 cột và không có gì update ngoài ID, có thể bỏ WHEN MATCHED.
    # Ví dụ đơn giản hơn cho WHEN MATCHED (luôn update nếu key khớp):
    # WHEN MATCHED THEN UPDATE SET {update_set_clause}
    # Tuy nhiên, với các bảng dimension đơn giản (ID, Name), nếu ID (hash) đã khớp, Name thường cũng khớp.
    # Nên có thể chỉ cần WHEN NOT MATCHED.
    
    # Phiên bản MERGE đơn giản hơn, chỉ INSERT nếu chưa có (phù hợp nếu ID là hash và không có update thuộc tính)
    simple_merge_sql = f"""
    MERGE INTO {full_table_name} target
    USING new_{table_name_in_db} source
    ON target.{pk_column_name} = source.{pk_column_name}
    WHEN NOT MATCHED
    THEN INSERT ({insert_columns}) VALUES ({insert_values})
    """
    
    print(f"Đang MERGE dữ liệu vào bảng: {full_table_name}")
    try:
        print(f"Executing MERGE SQL: \n{simple_merge_sql}") # In ra câu lệnh SQL để debug
        spark.sql(simple_merge_sql)
        print(f"MERGE dữ liệu vào bảng '{full_table_name}' thành công.")
    except Exception as e:
        print(f"Lỗi khi MERGE vào bảng '{full_table_name}': {e}")
        raise
        
def write_iceberg_table_daily_update(df_to_write, table_name_in_db, mode, db_name=CLEAN_DATABASE_NAME, catalog_name=clean_catalog_name, partition_cols=None, delete_condition_col=None, delete_values_df=None):
    full_table_name = f"`{catalog_name}`.`{db_name}`.`{table_name_in_db}`"
    print(f"Đang ghi (Daily Update) vào bảng: {full_table_name} với mode '{mode}'")
    try:
        if mode == "overwrite_partitions":
            if not partition_cols:
                raise ValueError("partition_cols phải được cung cấp cho mode 'overwrite_partitions'")
            df_to_write.writeTo(full_table_name) \
                .tableProperty("write.format.default", "parquet") \
                .partitionedBy(*partition_cols) \
                .overwritePartitions()
        elif mode == "delete_append":
            if not delete_condition_col or delete_values_df is None:
                raise ValueError("delete_condition_col và delete_values_df phải được cung cấp cho mode 'delete_append'")
            
            # Tạo danh sách giá trị để xóa (đảm bảo là list của Python)
            # Ví dụ delete_values_df có cột là "ArticleID"
            values_to_delete = [row[delete_condition_col] for row in delete_values_df.select(delete_condition_col).distinct().collect()]
            
            if values_to_delete:
                # Chuyển danh sách thành chuỗi SQL 'val1', 'val2', ...
                values_str = ", ".join([f"'{val}'" if isinstance(val, str) else str(val) for val in values_to_delete])
                delete_sql = f"DELETE FROM {full_table_name} WHERE {delete_condition_col} IN ({values_str})"
                print(f"Thực hiện DELETE: {delete_sql}")
                spark.sql(delete_sql)
                print(f"Đã DELETE dữ liệu cũ từ {full_table_name} cho {delete_condition_col} liên quan.")
            else:
                print(f"Không có giá trị nào để DELETE trong {full_table_name} cho {delete_condition_col}.")

            # Sau đó append dữ liệu mới
            df_to_write.writeTo(full_table_name) \
                .tableProperty("write.format.default", "parquet") \
                .append()
        else:
            raise ValueError(f"Chế độ ghi '{mode}' không được hỗ trợ cho Daily Update trong hàm này.")
        print(f"Đã ghi dữ liệu vào bảng '{full_table_name}' thành công.")
    except Exception as e:
        print(f"Lỗi khi ghi vào bảng '{full_table_name}': {e}")
        raise

# --- Ghi dữ liệu vào các bảng CLEAN cho DAILY UPDATE ---
print("\n--- Bắt đầu ghi dữ liệu DAILY UPDATE vào các bảng CLEAN ---")

# 1. Dimension-like tables: MERGE INTO
merge_into_iceberg_table(authors_df_new, "authors", "AuthorID")
merge_into_iceberg_table(topics_df_new, "topics", "TopicID")
merge_into_iceberg_table(subtopics_df_new, "subtopics", "SubTopicID") # Cần đảm bảo TopicID trong subtopics_df_new là đúng
merge_into_iceberg_table(keywords_dim_df_new, "keywords", "KeywordID")
merge_into_iceberg_table(references_dim_df_new, "references_table", "ReferenceID")

# 2. articles: overwritePartitions
# articles_to_write_df_daily chỉ chứa dữ liệu 7 ngày
write_iceberg_table_daily_update(articles_to_write_df_daily, "articles", 
                                 mode="overwrite_partitions", 
                                 partition_cols=["PublicationDate"])

# 3. Bảng nối và comments: DELETE rồi APPEND
# Cần DataFrame chứa các ArticleID của 7 ngày để làm điều kiện DELETE
article_ids_7_days_df = articles_to_write_df_daily.select("ArticleID").distinct()

write_iceberg_table_daily_update(article_keywords_final_df_daily, "article_keywords", 
                                 mode="delete_append", 
                                 delete_condition_col="ArticleID", 
                                 delete_values_df=article_ids_7_days_df)

write_iceberg_table_daily_update(article_references_final_df_daily, "article_references", 
                                 mode="delete_append", 
                                 delete_condition_col="ArticleID", 
                                 delete_values_df=article_ids_7_days_df)

if 'comments_to_write_df_daily' in locals() and comments_to_write_df_daily.count() > 0 :
    write_iceberg_table_daily_update(comments_to_write_df_daily, "comments", 
                                     mode="delete_append", 
                                     delete_condition_col="ArticleID", 
                                     delete_values_df=article_ids_7_days_df)
else:
    print("Không có dữ liệu comments để ghi (Daily Update).")

if 'comment_interactions_to_write_df_daily' in locals() and comment_interactions_to_write_df_daily.count() > 0:
    # Để delete comment_interactions, cần CommentID của các comment thuộc 7 ngày đó
    comment_ids_7_days_df = comments_to_write_df_daily.select("CommentID").distinct() # Giả sử comments_to_write_df_daily chỉ chứa comment của 7 ngày
    write_iceberg_table_daily_update(comment_interactions_to_write_df_daily, "comment_interactions", 
                                     mode="delete_append", 
                                     delete_condition_col="CommentID", 
                                     delete_values_df=comment_ids_7_days_df)
else:
    print("Không có dữ liệu comment_interactions để ghi (Daily Update).")


print("\n--- Hoàn thành quy trình ETL DAILY UPDATE từ Raw sang Clean ---")
spark.stop()
