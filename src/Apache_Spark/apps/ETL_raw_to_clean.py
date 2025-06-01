from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_timestamp, current_timestamp,
    lit, when, expr, sha2, concat_ws, coalesce, udf, from_json, date_format,
    trim, broadcast # Đã thêm trim và broadcast
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType, MapType
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import argparse
import time
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import traceback # Thêm để in traceback lỗi chi tiết

load_dotenv()

# --- Cấu hình MinIO, Nessie ---
minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
nessie_uri = os.getenv("NESSIE_URI")
nessie_default_branch = os.getenv("NESSIE_DEFAULT_BRANCH")

# --- Cấu hình Catalog và Database cho CLEAN zone ---
clean_catalog_name = "nessie-clean-news"
clean_catalog_warehouse_path = "s3a://clean-news-lakehouse/nessie_clean_news_warehouse"
CLEAN_DATABASE_NAME = "news_clean_db"

app_name = "NewsETLRawToClean" # Sẽ được dùng làm JOB_NAME cho Prometheus

arg_parser = argparse.ArgumentParser(description="Tham số cho ngày bắt đầu và kết thúc cho ETL")
arg_parser.add_argument(
    "--etl-start-date",
    type=str,
    default=None,
    help="Start date (YYYY-MM-DD). Mặc định (last 7 days)."
)
arg_parser.add_argument(
    "--etl-end-date",
    type=str,
    default=None,
    help="End date (YYYY-MM-DD). Mặc định (last 7 days)."
)
arg_parser.add_argument(
    "--airflow-run-id",
    type=str,
    default=f"run_raw_clean_{datetime.now().strftime('%Y%m%d%H%M%S')}",
    help="Airflow Run ID hoặc một instance ID duy nhất cho lần chạy job."
)
args = arg_parser.parse_args()

# --- CẤU HÌNH THỜI GIAN ETL ---
ETL_START_DATE_STR = args.etl_start_date
ETL_END_DATE_STR = args.etl_end_date
RAW_BASE_S3_PATH = "s3a://raw-news-lakehouse"

# --- Cấu hình Prometheus ---
PROMETHEUS_PUSHGATEWAY_URL = "http://pushgateway:9091"
JOB_NAME = app_name
INSTANCE_ID = args.airflow_run_id

registry = CollectorRegistry()
METRIC_PREFIX = "etl"

g_job_duration = Gauge(f'{METRIC_PREFIX}_job_duration_seconds', 'Tổng thời gian chạy của job ETL', registry=registry)
g_job_status = Gauge(f'{METRIC_PREFIX}_job_status', 'Trạng thái kết thúc job (1=thành công, 0=thất bại)', registry=registry)
g_job_last_success_ts = Gauge(f'{METRIC_PREFIX}_job_last_success_timestamp', 'Unix timestamp của lần chạy job thành công cuối cùng', registry=registry)
g_processed_date_start_ts = Gauge(f'{METRIC_PREFIX}_job_processed_date_range_start_timestamp', 'Unix timestamp của ngày bắt đầu xử lý', registry=registry)
g_processed_date_end_ts = Gauge(f'{METRIC_PREFIX}_job_processed_date_range_end_timestamp', 'Unix timestamp của ngày kết thúc xử lý', registry=registry)
g_valid_url_records_count = Gauge(f'{METRIC_PREFIX}_valid_url_records_processed_count', 'Số lượng bản ghi có URL hợp lệ được xử lý', registry=registry)
g_read_stage_duration = Gauge(f'{METRIC_PREFIX}_read_stage_duration_seconds', 'Thời gian đọc và lọc dữ liệu raw ban đầu', registry=registry)
g_articles_records_to_write = Gauge(f'{METRIC_PREFIX}_articles_records_to_write_count', 'Số lượng bản ghi articles chuẩn bị để ghi/merge', registry=registry)
g_articles_write_duration = Gauge(f'{METRIC_PREFIX}_articles_write_duration_seconds', 'Thời gian ghi/merge bảng articles', registry=registry)
g_articles_by_pub_date_count = Gauge(
    f'{METRIC_PREFIX}_articles_by_publication_date_count',
    'Số lượng bài báo theo ngày xuất bản',
    ['publication_date'],
    registry=registry
)
g_articles_by_topic_count = Gauge(
    f'{METRIC_PREFIX}_articles_by_topic_count',
    'Số lượng bài báo theo chủ đề',
    ['topic_name'],
    registry=registry
)

def push_metrics_to_gateway():
    try:
        push_to_gateway(
            PROMETHEUS_PUSHGATEWAY_URL,
            job=JOB_NAME,
            registry=registry,
            grouping_key={'instance': INSTANCE_ID}
        )
        print(f"SUCCESS: Đã push metrics lên Pushgateway: {PROMETHEUS_PUSHGATEWAY_URL} cho job {JOB_NAME}, instance {INSTANCE_ID}")
    except Exception as e:
        print(f"ERROR: Không thể push metrics lên Pushgateway: {e}")

overall_job_start_time = time.time()
job_succeeded_flag = False

spark_builder = SparkSession.builder.appName(app_name)

# Cấu hình S3A và Spark SQL Extensions
spark_builder = spark_builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{clean_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{clean_catalog_name}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config(f"spark.sql.catalog.{clean_catalog_name}.uri", nessie_uri) \
    .config(f"spark.sql.catalog.{clean_catalog_name}.ref", nessie_default_branch) \
    .config(f"spark.sql.catalog.{clean_catalog_name}.warehouse", clean_catalog_warehouse_path) \
    .config(f"spark.sql.catalog.{clean_catalog_name}.authentication.type", "NONE") \
    .config("spark.sql.adaptive.enabled", "true") # TỐI ƯU: Bật Adaptive Query Execution (cho Spark 3.x+)

# Khởi tạo actual_start_date_for_metric và actual_end_date_for_metric
actual_start_date_for_metric = None
actual_end_date_for_metric = None

try:
    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"SparkSession đã được khởi tạo cho ETL: {app_name}.")

    # --- HÀM TIỆN ÍCH ---
    def generate_surrogate_key(*cols_to_hash):
        return sha2(concat_ws("||", *cols_to_hash), 256)

    # TỐI ƯU: Hàm được sửa đổi để trả về ngày
    def get_s3_paths_for_date_range(base_s3_path, start_date_str=None, end_date_str=None):
        paths = []
        _actual_start_date = None
        _actual_end_date = None
        date_format_pattern = "%Y-%m-%d"

        if start_date_str and end_date_str:
            try:
                _actual_start_date = datetime.strptime(start_date_str, date_format_pattern)
                _actual_end_date = datetime.strptime(end_date_str, date_format_pattern)
                print(f"Sử dụng khoảng thời gian tùy chỉnh: {start_date_str} đến {end_date_str}")
            except ValueError:
                print(f"Lỗi định dạng ngày: {start_date_str}, {end_date_str}. Vui lòng dùng YYYY-MM-DD.")
                raise
        else:
            _actual_end_date = datetime.now() - timedelta(days=1)
            _actual_start_date = _actual_end_date - timedelta(days=6)
            print(f"Sử dụng khoảng thời gian mặc định: 7 ngày gần nhất, từ {_actual_start_date.strftime(date_format_pattern)} đến {_actual_end_date.strftime(date_format_pattern)}")

        current_date = _actual_start_date
        while current_date <= _actual_end_date:
            path = f"{base_s3_path}/{current_date.strftime('%Y/%m/%d')}/*.jsonl"
            paths.append(path)
            current_date += timedelta(days=1)
        return paths, _actual_start_date, _actual_end_date


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

    # Gọi hàm đã sửa đổi
    raw_data_paths, actual_start_date_for_metric, actual_end_date_for_metric = \
        get_s3_paths_for_date_range(RAW_BASE_S3_PATH, ETL_START_DATE_STR, ETL_END_DATE_STR)

    if not raw_data_paths:
        print("Không có đường dẫn dữ liệu nào được tạo cho khoảng thời gian đã chọn. Dừng job.")
        raise SystemExit("Dừng job vì không có đường dẫn dữ liệu.")

    read_stage_start_time = time.time()
    try:
        print(f"Đang đọc dữ liệu raw từ các đường dẫn: {raw_data_paths}")
        df_before_url_filter = spark.read.schema(raw_schema).json(raw_data_paths)
        
        # TỐI ƯU: Sử dụng trim để filter URL mạnh mẽ hơn
        raw_df_initial = df_before_url_filter.filter(col("url").isNotNull() & (trim(col("url")) != ""))
        
        count_raw_initial = raw_df_initial.count()
        g_valid_url_records_count.set(count_raw_initial)

        print(f"Đã đọc dữ liệu raw thành công. Số lượng bản ghi raw ban đầu (có URL): {count_raw_initial}")
        if count_raw_initial == 0:
            print("Không có dữ liệu raw nào được đọc hoặc tất cả bản ghi không có URL. Dừng job.")
            raise SystemExit("Dừng job vì không có dữ liệu raw hợp lệ.")
    except Exception as e:
        print(f"Lỗi khi đọc dữ liệu raw: {e}")
        raise
    finally:
        read_stage_duration_val = time.time() - read_stage_start_time
        g_read_stage_duration.set(read_stage_duration_val)

    raw_df_with_article_id = raw_df_initial.withColumn("ArticleID", generate_surrogate_key(col("url")))
    # raw_df_with_article_id.cache() # Cân nhắc cache nếu raw_df_with_article_id được dùng nhiều và bộ nhớ cho phép

    print("--- BẮT ĐẦU QUÁ TRÌNH TRANSFORM ---")

    # --- Xử lý bảng AUTHORS ---
    print("\n--- Xử lý bảng AUTHORS ---")
    authors_intermediate_df = raw_df_with_article_id.select(col("tac_gia").alias("AuthorName")) \
        .filter(col("AuthorName").isNotNull() & (trim(col("AuthorName")) != "")) \
        .distinct()
    authors_df = authors_intermediate_df \
        .withColumn("AuthorID", generate_surrogate_key(col("AuthorName"))) \
        .select("AuthorID", "AuthorName") \
        .dropDuplicates(["AuthorID"])

    # --- Xử lý bảng TOPICS ---
    print("\n--- Xử lý bảng TOPICS ---")
    topics_intermediate_df = raw_df_with_article_id.select(col("topic").alias("TopicName")) \
        .filter(col("TopicName").isNotNull() & (trim(col("TopicName")) != "")) \
        .distinct()
    topics_df = topics_intermediate_df \
        .withColumn("TopicID", generate_surrogate_key(col("TopicName"))) \
        .select("TopicID", "TopicName") \
        .dropDuplicates(["TopicID"])

    # --- Xử lý bảng SUBTOPICS ---
    print("\n--- Xử lý bảng SUBTOPICS ---")
    subtopics_intermediate_df = raw_df_with_article_id \
        .filter(
            col("sub_topic").isNotNull() & (trim(col("sub_topic")) != "") & \
            col("topic").isNotNull() & (trim(col("topic")) != "")
        ) \
        .select(col("sub_topic").alias("SubTopicName"), col("topic").alias("ParentTopicName")) \
        .distinct()
    subtopics_df = subtopics_intermediate_df \
        .join(broadcast(topics_df), subtopics_intermediate_df.ParentTopicName == topics_df.TopicName, "inner") \
        .withColumn("SubTopicID", generate_surrogate_key(col("SubTopicName"), col("TopicID"))) \
        .select(col("SubTopicID"), col("SubTopicName"), col("TopicID")) \
        .dropDuplicates(["SubTopicID"])

    # --- Xử lý bảng KEYWORDS (Dimension) ---
    print("\n--- Xử lý bảng KEYWORDS (Dimension) ---")
    keywords_dim_intermediate_df = raw_df_with_article_id \
        .filter(col("tu_khoa").isNotNull() & (expr("size(tu_khoa) > 0"))) \
        .select(explode(col("tu_khoa")).alias("KeywordText")) \
        .filter(col("KeywordText").isNotNull() & (trim(col("KeywordText")) != "")) \
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
        .filter(col("ReferenceText").isNotNull() & (trim(col("ReferenceText")) != "")) \
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

    articles_joined_df = articles_base_aliased \
        .join(broadcast(authors_df.alias("auth")), col("base.tac_gia") == col("auth.AuthorName"), "left_outer") \
        .join(broadcast(topics_df.alias("top")), col("base.topic") == col("top.TopicName"), "left_outer") \
        .join(broadcast(subtopics_df.alias("sub")), # Giả sử subtopics_df cũng tương đối nhỏ
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

    articles_to_write_df.cache()
    count_articles_to_write = articles_to_write_df.count()
    g_articles_records_to_write.set(count_articles_to_write)

    if count_articles_to_write > 0:
        print("Đang tính toán số lượng bài báo theo ngày xuất bản...")
        articles_by_date_collected = articles_to_write_df \
            .withColumn("PublicationDateString", date_format(col("PublicationDate"), "yyyy-MM-dd")) \
            .groupBy("PublicationDateString") \
            .count() \
            .collect()
        for row in articles_by_date_collected:
            if row["PublicationDateString"]:
                g_articles_by_pub_date_count.labels(publication_date=row["PublicationDateString"]).set(row["count"])
        print("Đã set metric số lượng bài báo theo ngày xuất bản.")

        print("Đang tính toán số lượng bài báo theo chủ đề...")
        articles_by_topic_collected = articles_to_write_df \
            .join(broadcast(topics_df), articles_to_write_df.TopicID == topics_df.TopicID, "inner") \
            .groupBy(topics_df.TopicName) \
            .count() \
            .collect()
        for row in articles_by_topic_collected:
            if row["TopicName"]:
                g_articles_by_topic_count.labels(topic_name=row["TopicName"]).set(row["count"])
        print("Đã set metric số lượng bài báo theo chủ đề.")
    else:
        print("Không có bài báo nào để tính toán metrics theo ngày/chủ đề.")

    # --- Xử lý bảng ARTICLEKEYWORDS (Junction) ---
    print("\n--- Xử lý bảng ARTICLEKEYWORDS (Junction) ---")
    article_keywords_exploded_df = raw_df_with_article_id \
        .filter(col("tu_khoa").isNotNull() & (expr("size(tu_khoa) > 0"))) \
        .select(col("ArticleID"), explode(col("tu_khoa")).alias("KeywordText")) \
        .filter(col("KeywordText").isNotNull() & (trim(col("KeywordText")) != ""))

    article_keywords_final_df = article_keywords_exploded_df \
        .join(broadcast(keywords_dim_df), article_keywords_exploded_df.KeywordText == keywords_dim_df.KeywordText, "inner") \
        .select("ArticleID", "KeywordID") \
        .distinct()

    # --- Xử lý bảng ARTICLEREFERENCES (Junction) ---
    print("\n--- Xử lý bảng ARTICLEREFERENCES (Junction) ---")
    article_references_exploded_df = raw_df_with_article_id \
        .filter(col("tham_khao").isNotNull() & (expr("size(tham_khao) > 0"))) \
        .select(col("ArticleID"), explode(col("tham_khao")).alias("ReferenceText")) \
        .filter(col("ReferenceText").isNotNull() & (trim(col("ReferenceText")) != ""))

    article_references_final_df = article_references_exploded_df \
        .join(broadcast(references_dim_df), article_references_exploded_df.ReferenceText == references_dim_df.ReferenceText, "inner") \
        .select("ArticleID", "ReferenceID") \
        .distinct()

    # --- Xử lý bảng COMMENTS và COMMENTINTERACTIONS ---
    print("\n--- Xử lý bảng COMMENTS và COMMENTINTERACTIONS ---")
    
    # TỐI ƯU: Thay thế UDF bằng hàm Spark SQL
    comments_parsed_base_df = raw_df_with_article_id \
        .select("ArticleID", "top_binh_luan") \
        .withColumn("parsed_comments_str",
            when(
                (col("top_binh_luan").isNull()) |
                (trim(col("top_binh_luan")) == "") |
                (col("top_binh_luan").contains("Không tìm thấy bình luận")),
                lit(None).cast(StringType())
            ).otherwise(col("top_binh_luan"))
        ) \
        .filter(col("parsed_comments_str").isNotNull())

    comments_exploded_df = comments_parsed_base_df \
        .withColumn("parsed_comments_array", from_json(col("parsed_comments_str"), top_binh_luan_array_schema)) \
        .filter(col("parsed_comments_array").isNotNull()) \
        .select(col("ArticleID"), explode(col("parsed_comments_array")).alias("comment_data"))

    comments_schema = StructType([
        StructField("CommentID", StringType(), True), StructField("ArticleID", StringType(), True),
        StructField("CommenterName", StringType(), True), StructField("CommentContent", StringType(), True),
        StructField("TotalLikes", IntegerType(), True), StructField("LastProcessedTimestamp", TimestampType(), True)
    ])
    comment_interactions_schema = StructType([
        StructField("CommentInteractionID", StringType(), True), StructField("CommentID", StringType(), True),
        StructField("InteractionType", StringType(), True), StructField("InteractionCount", IntegerType(), True),
        StructField("LastProcessedTimestamp", TimestampType(), True)
    ])

    if comments_exploded_df.rdd.isEmpty():
        print("Không có dữ liệu bình luận hợp lệ để xử lý.")
        comments_to_write_df = spark.createDataFrame([], comments_schema)
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

        if unique_comments_for_interactions.rdd.isEmpty():
            print("Không có dữ liệu chi tiết tương tác bình luận.")
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
                                     partition_cols=None, create_table_if_not_exists=True,
                                     is_articles_table=False):
        full_table_name = f"`{catalog_name}`.`{db_name}`.`{table_name_in_db}`"
        # TỐI ƯU: Tên view tạm thời duy nhất hơn
        temp_view_name = f"{table_name_in_db}_source_view_{INSTANCE_ID}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

        write_articles_start_time_local = None # Đổi tên để tránh xung đột với biến global tiềm năng (dù không có)
        if is_articles_table:
            write_articles_start_time_local = time.time()

        # TỐI ƯU: Kiểm tra DF rỗng trước khi thực hiện thao tác ghi
        if df_to_write.rdd.isEmpty():
            print(f"DataFrame cho bảng '{full_table_name}' rỗng. Bỏ qua ghi.")
            if is_articles_table:
                 g_articles_write_duration.set(0) # Vẫn set duration là 0 nếu là bảng articles nhưng rỗng
            return

        df_to_write.createOrReplaceTempView(temp_view_name)
        print(f"Đang chuẩn bị MERGE INTO bảng: {full_table_name} từ view {temp_view_name}")

        try:
            # TỐI ƯU: Xử lý dấu gạch dưới trong tên bảng cho LIKE
            table_exists_query_table_name = table_name_in_db.replace("_", "\\_") # Escape for SQL LIKE
            table_exists = spark.sql(f"SHOW TABLES IN `{catalog_name}`.`{db_name}` LIKE '{table_exists_query_table_name}'").count() > 0


            if not table_exists:
                if create_table_if_not_exists:
                    print(f"Bảng {full_table_name} chưa tồn tại. Sẽ tạo bảng và ghi dữ liệu.")
                    writer_builder = df_to_write.writeTo(full_table_name) \
                        .tableProperty("write.format.default", "parquet") \
                        .tableProperty("write.nessie.identifier-properties.enabled", "true")
                    if partition_cols:
                        writer_builder = writer_builder.partitionedBy(*partition_cols)
                    writer_builder.create()
                    print(f"Đã tạo và ghi dữ liệu vào bảng '{full_table_name}' thành công.")
                else:
                    raise RuntimeError(f"Bảng {full_table_name} không tồn tại và create_table_if_not_exists=False.")
            else: 
                merge_condition_parts = [f"target.{pk_col} = source.{pk_col}" for pk_col in primary_key_cols]
                merge_condition = " AND ".join(merge_condition_parts)

                # Sử dụng "UPDATE SET *" và "INSERT *" đơn giản hơn nếu schema nguồn và đích (cho các cột liên quan) khớp nhau.
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
            if is_articles_table and write_articles_start_time_local is not None:
                g_articles_write_duration.set(time.time() - write_articles_start_time_local)
            raise
        finally:
            spark.catalog.dropTempView(temp_view_name)
            print(f"Đã xóa temp view: {temp_view_name}")
            if is_articles_table and write_articles_start_time_local is not None:
                # Đảm bảo chỉ set một lần nếu lỗi xảy ra trong khối try
                current_metric_value = None
                try: # Cố gắng lấy giá trị hiện tại một cách an toàn
                    current_metric_value = g_articles_write_duration._value
                except AttributeError: # Nếu _value không tồn tại (metric chưa được set)
                    pass
                if current_metric_value is None: # Chỉ set nếu chưa được set bởi khối except ở trên
                     g_articles_write_duration.set(time.time() - write_articles_start_time_local)


    # --- GHI DỮ LIỆU VÀO CÁC BẢNG CLEAN ---
    print("\n--- Bắt đầu ghi dữ liệu vào các bảng CLEAN sử dụng MERGE ---")

    write_iceberg_table_with_merge(authors_df, "authors", primary_key_cols=["AuthorID"])
    write_iceberg_table_with_merge(topics_df, "topics", primary_key_cols=["TopicID"])
    write_iceberg_table_with_merge(subtopics_df, "subtopics", primary_key_cols=["SubTopicID"])
    write_iceberg_table_with_merge(keywords_dim_df, "keywords", primary_key_cols=["KeywordID"])
    write_iceberg_table_with_merge(references_dim_df, "references_table", primary_key_cols=["ReferenceID"])
    
    if count_articles_to_write > 0:
        write_iceberg_table_with_merge(articles_to_write_df, "articles",
                                     primary_key_cols=["ArticleID"],
                                     partition_cols=["PublicationDate"],
                                     is_articles_table=True)
    else:
        print("Không có dữ liệu articles để ghi.")
        g_articles_write_duration.set(0)

    write_iceberg_table_with_merge(article_keywords_final_df, "article_keywords", primary_key_cols=["ArticleID", "KeywordID"])
    write_iceberg_table_with_merge(article_references_final_df, "article_references", primary_key_cols=["ArticleID", "ReferenceID"])
    
    write_iceberg_table_with_merge(comments_to_write_df, "comments", primary_key_cols=["CommentID"])
    write_iceberg_table_with_merge(comment_interactions_to_write_df, "comment_interactions", primary_key_cols=["CommentInteractionID"])

    print("\n--- Hoàn thành quy trình ETL từ Raw sang Clean ---")
    job_succeeded_flag = True

except SystemExit as se:
    print(f"Job dừng chủ động: {se}")
    job_succeeded_flag = False 
except Exception as e:
    print(f"LỖI CHÍNH TRONG QUÁ TRÌNH ETL: {e}")
    traceback.print_exc() # TỐI ƯU: In traceback đầy đủ
    job_succeeded_flag = False
finally:
    overall_job_duration = time.time() - overall_job_start_time
    g_job_duration.set(overall_job_duration)

    # TỐI ƯU: Đơn giản hóa logic set g_job_status
    if job_succeeded_flag:
        g_job_status.set(1) # Thành công
        g_job_last_success_ts.set(int(time.time()))
    else:
        g_job_status.set(0) # Thất bại (bao gồm cả SystemExit và Exception khác)

    if actual_start_date_for_metric:
        g_processed_date_start_ts.set(int(actual_start_date_for_metric.timestamp()))
    if actual_end_date_for_metric:
        g_processed_date_end_ts.set(int(actual_end_date_for_metric.timestamp()))

    push_metrics_to_gateway()

    if 'spark' in locals() and spark:
        # TỐI ƯU: Kiểm tra storageLevel trước khi unpersist
        if 'articles_to_write_df' in locals() and \
           hasattr(articles_to_write_df, 'storageLevel') and \
           articles_to_write_df.storageLevel.useMemory:
                 articles_to_write_df.unpersist()
                 print("Đã unpersist articles_to_write_df.")
        spark.stop()
        print("Spark session đã được dừng.")   
