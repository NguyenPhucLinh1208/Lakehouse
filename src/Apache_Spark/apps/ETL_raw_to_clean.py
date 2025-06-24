from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_timestamp, current_timestamp,
    lit, when, expr, sha2, concat_ws, coalesce, udf, from_json, date_format,
    trim, broadcast, input_file_name, regexp_extract, concat, to_date
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType, MapType
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import argparse
import time
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import traceback
import pytz

load_dotenv()

minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
nessie_uri = os.getenv("NESSIE_URI")
nessie_default_branch = os.getenv("NESSIE_DEFAULT_BRANCH")

clean_catalog_name = "nessie-clean-news"
clean_catalog_warehouse_path = "s3a://clean-news-lakehouse/nessie_clean_news_warehouse"
CLEAN_DATABASE_NAME = "news_clean_db"

app_name = "NewsETLRawToClean"

VIETNAM_TZ = pytz.timezone('Asia/Ho_Chi_Minh')
DATE_FORMAT_PATTERN = "%Y-%m-%d"

arg_parser = argparse.ArgumentParser(description="Tham số cho ngày bắt đầu và kết thúc cho ETL")
arg_parser.add_argument("--etl-start-date", type=str, default=None)
arg_parser.add_argument("--etl-end-date", type=str, default=None)
arg_parser.add_argument("--airflow-run-id", type=str, default=f"run_raw_clean_{datetime.now().strftime('%Y%m%d%H%M%S')}")
args = arg_parser.parse_args()

ETL_START_DATE_STR = args.etl_start_date
ETL_END_DATE_STR = args.etl_end_date
RAW_BASE_S3_PATH = "s3a://raw-news-lakehouse"

PROMETHEUS_PUSHGATEWAY_URL = "http://pushgateway:9091"
JOB_NAME = app_name
INSTANCE_ID = args.airflow_run_id

registry = CollectorRegistry()
METRIC_PREFIX = "etl"

g_job_duration = Gauge(f'{METRIC_PREFIX}_job_duration_seconds', 'Tổng thời gian chạy của job ETL', registry=registry)
g_job_status = Gauge(f'{METRIC_PREFIX}_job_status', 'Trạng thái kết thúc job (1=thành công, 0=thất bại)', registry=registry)
g_job_last_success_ts = Gauge(f'{METRIC_PREFIX}_job_last_success_timestamp', 'Unix timestamp của lần chạy job thành công cuối cùng', registry=registry)
g_processed_date_start_ts = Gauge(f'{METRIC_PREFIX}_job_processed_date_range_start_timestamp', 'Unix timestamp của ngày bắt đầu xử lý (0h giờ VN)', registry=registry)
g_processed_date_end_ts = Gauge(f'{METRIC_PREFIX}_job_processed_date_range_end_timestamp', 'Unix timestamp của ngày kết thúc xử lý (0h giờ VN)', registry=registry)
g_valid_url_records_count = Gauge(f'{METRIC_PREFIX}_valid_url_records_processed_count', 'Số lượng bản ghi có URL hợp lệ được xử lý', registry=registry)
g_read_stage_duration = Gauge(f'{METRIC_PREFIX}_read_stage_duration_seconds', 'Thời gian đọc và lọc dữ liệu raw ban đầu', registry=registry)
g_articles_records_to_write = Gauge(f'{METRIC_PREFIX}_articles_records_to_write_count', 'Số lượng bản ghi articles chuẩn bị để ghi/merge', registry=registry)
g_total_write_duration = Gauge(f'{METRIC_PREFIX}_total_write_duration_seconds', 'Tổng thời gian ghi/merge tất cả các bảng', registry=registry)
g_transform_stage_duration = Gauge(f'{METRIC_PREFIX}_transform_stage_duration_seconds', 'Tổng thời gian cho giai đoạn transform dữ liệu', registry=registry)
g_articles_by_pub_date_count = Gauge(
    f'{METRIC_PREFIX}_articles_by_publication_date_count',
    'Số lượng bài báo theo ngày xuất bản (giờ VN)',
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
total_write_duration_accumulator = 0.0

spark_builder = SparkSession.builder.appName(app_name)

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
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") \
    .config("spark.sql.shuffle.partitions", "4")

actual_start_date_for_metric = None
actual_end_date_for_metric = None

try:
    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"SparkSession đã được khởi tạo cho ETL: {app_name} với múi giờ session: {spark.conf.get('spark.sql.session.timeZone')}.")

    def generate_surrogate_key(*cols_to_hash):
        return sha2(concat_ws("||", *cols_to_hash), 256)

    def get_s3_paths_for_date_range(base_s3_path, start_date_str=None, end_date_str=None):
        paths = []
        _actual_start_date_aware = None
        _actual_end_date_aware = None

        if start_date_str and end_date_str:
            try:
                start_date_naive = datetime.strptime(start_date_str, DATE_FORMAT_PATTERN)
                end_date_naive = datetime.strptime(end_date_str, DATE_FORMAT_PATTERN)
                _actual_start_date_aware = VIETNAM_TZ.localize(start_date_naive)
                _actual_end_date_aware = VIETNAM_TZ.localize(end_date_naive)
                print(f"Sử dụng khoảng thời gian tùy chỉnh (giờ VN): {_actual_start_date_aware} đến {_actual_end_date_aware}")
            except ValueError:
                print(f"Lỗi định dạng ngày: '{start_date_str}', '{end_date_str}'. Vui lòng dùng định dạng '{DATE_FORMAT_PATTERN}'.")
                raise
        else:
            now_in_vietnam = datetime.now(VIETNAM_TZ)
            _actual_end_date_aware = (now_in_vietnam - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            _actual_start_date_aware = _actual_end_date_aware - timedelta(days=6)
            print(f"Sử dụng khoảng thời gian mặc định (giờ VN): 7 ngày gần nhất, từ {_actual_start_date_aware.strftime(DATE_FORMAT_PATTERN)} đến {_actual_end_date_aware.strftime(DATE_FORMAT_PATTERN)}")

        current_date_aware = _actual_start_date_aware
        while current_date_aware <= _actual_end_date_aware:
            path = f"{base_s3_path}/{current_date_aware.strftime('%Y/%m/%d')}/*.jsonl"
            paths.append(path)
            current_date_aware += timedelta(days=1)
        return paths, _actual_start_date_aware, _actual_end_date_aware

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
    
    raw_data_paths, actual_start_date_for_metric, actual_end_date_for_metric = \
        get_s3_paths_for_date_range(RAW_BASE_S3_PATH, ETL_START_DATE_STR, ETL_END_DATE_STR)

    if not raw_data_paths:
        print("Không có đường dẫn dữ liệu nào được tạo cho khoảng thời gian đã chọn. Dừng job.")
        raise SystemExit("Dừng job vì không có đường dẫn dữ liệu.")

    read_stage_start_time = time.time()
    try:
        print(f"Đang đọc dữ liệu raw từ các đường dẫn: {raw_data_paths}")
        df_before_url_filter = spark.read.schema(raw_schema).json(raw_data_paths)
        
        df_with_path = df_before_url_filter.withColumn("input_path", input_file_name())

        raw_df_initial = df_with_path.filter(col("url").isNotNull() & (trim(col("url")) != ""))
        raw_df_initial = raw_df_initial.coalesce(4)
        
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

    raw_df_with_article_id = raw_df_initial.withColumn("ArticleID", generate_surrogate_key(col("url"))).cache()

    print("--- BẮT ĐẦU QUÁ TRÌNH TRANSFORM ---")
    transform_stage_start_time = time.time()

    print("\n--- Xử lý bảng AUTHORS ---")
    authors_df = raw_df_with_article_id.select(col("tac_gia").alias("AuthorName")) \
        .filter(col("AuthorName").isNotNull() & (trim(col("AuthorName")) != "")) \
        .distinct() \
        .withColumn("AuthorID", generate_surrogate_key(col("AuthorName"))) \
        .select("AuthorID", "AuthorName") \
        .cache()

    print("\n--- Xử lý bảng TOPICS ---")
    topics_df = raw_df_with_article_id.select(col("topic").alias("TopicName")) \
        .filter(col("TopicName").isNotNull() & (trim(col("TopicName")) != "")) \
        .distinct() \
        .withColumn("TopicID", generate_surrogate_key(col("TopicName"))) \
        .select("TopicID", "TopicName") \
        .cache()

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
        .cache()

    print("\n--- Xử lý bảng KEYWORDS (Dimension) ---")
    keywords_dim_df = raw_df_with_article_id \
        .filter(col("tu_khoa").isNotNull() & (expr("size(tu_khoa) > 0"))) \
        .select(explode(col("tu_khoa")).alias("KeywordText")) \
        .filter(col("KeywordText").isNotNull() & (trim(col("KeywordText")) != "")) \
        .distinct() \
        .withColumn("KeywordID", generate_surrogate_key(col("KeywordText"))) \
        .select("KeywordID", "KeywordText") \
        .cache()

    print("\n--- Xử lý bảng REFERENCES_TABLE (Dimension) ---")
    references_dim_df = raw_df_with_article_id \
        .filter(col("tham_khao").isNotNull() & (expr("size(tham_khao) > 0"))) \
        .select(explode(col("tham_khao")).alias("ReferenceText")) \
        .filter(col("ReferenceText").isNotNull() & (trim(col("ReferenceText")) != "")) \
        .distinct() \
        .withColumn("ReferenceID", generate_surrogate_key(col("ReferenceText"))) \
        .select("ReferenceID", "ReferenceText") \
        .cache()

    print("\n--- Xử lý bảng ARTICLES ---")
    path_regex = r".*/(\d{4})/(\d{2})/(\d{2})/.*"
    articles_base_transformed_df = raw_df_with_article_id \
        .withColumn("date_str_from_path",
            concat(
                regexp_extract(col("input_path"), path_regex, 1), lit("-"),
                regexp_extract(col("input_path"), path_regex, 2), lit("-"),
                regexp_extract(col("input_path"), path_regex, 3)
            )
        ) \
        .withColumn("PublicationDate",
            to_date(col("date_str_from_path"), "yyyy-MM-dd").cast(TimestampType())
        ) \
        .withColumn("OpinionCount", coalesce(col("y_kien").cast(IntegerType()), lit(0)))

    articles_base_aliased = articles_base_transformed_df.alias("base")

    articles_joined_df = articles_base_aliased \
        .join(broadcast(authors_df.alias("auth")), col("base.tac_gia") == col("auth.AuthorName"), "left_outer") \
        .join(broadcast(topics_df.alias("top")), col("base.topic") == col("top.TopicName"), "left_outer") \
        .join(broadcast(subtopics_df.alias("sub")),
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
    
    subtopics_df.unpersist(); print("Đã giải phóng cache cho: subtopics_df")
    
    if count_articles_to_write > 0:
        print("Đang tính toán số lượng bài báo theo ngày xuất bản (giờ VN)...")
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

    print("\n--- Xử lý bảng ARTICLEKEYWORDS (Junction) ---")
    article_keywords_exploded_df = raw_df_with_article_id \
        .filter(col("tu_khoa").isNotNull() & (expr("size(tu_khoa) > 0"))) \
        .select(col("ArticleID"), explode(col("tu_khoa")).alias("KeywordText")) \
        .filter(col("KeywordText").isNotNull() & (trim(col("KeywordText")) != ""))

    article_keywords_final_df = article_keywords_exploded_df \
        .join(broadcast(keywords_dim_df), article_keywords_exploded_df.KeywordText == keywords_dim_df.KeywordText, "inner") \
        .select("ArticleID", "KeywordID") \
        .distinct()

    print("\n--- Xử lý bảng ARTICLEREFERENCES (Junction) ---")
    article_references_exploded_df = raw_df_with_article_id \
        .filter(col("tham_khao").isNotNull() & (expr("size(tham_khao) > 0"))) \
        .select(col("ArticleID"), explode(col("tham_khao")).alias("ReferenceText")) \
        .filter(col("ReferenceText").isNotNull() & (trim(col("ReferenceText")) != ""))

    article_references_final_df = article_references_exploded_df \
        .join(broadcast(references_dim_df), article_references_exploded_df.ReferenceText == references_dim_df.ReferenceText, "inner") \
        .select("ArticleID", "ReferenceID") \
        .distinct()

    print("\n--- Xử lý bảng COMMENTS và COMMENTINTERACTIONS ---")
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

    if comments_exploded_df.isEmpty():
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

        if unique_comments_for_interactions.isEmpty():
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

    def write_iceberg_table_with_merge(df_to_write, table_name_in_db, primary_key_cols,
                                       db_name=CLEAN_DATABASE_NAME, catalog_name=clean_catalog_name,
                                       partition_cols=None, create_table_if_not_exists=True):
        
        global total_write_duration_accumulator
        
        full_table_name = f"`{catalog_name}`.`{db_name}`.`{table_name_in_db}`"
        temp_view_name = f"{table_name_in_db}_source_view_{INSTANCE_ID}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

        write_start_time_local = time.time()

        try:
            if df_to_write.isEmpty():
                print(f"DataFrame cho bảng '{full_table_name}' rỗng. Bỏ qua ghi.")
                return

            df_to_write.createOrReplaceTempView(temp_view_name)
            print(f"Đang chuẩn bị MERGE INTO bảng: {full_table_name} từ view {temp_view_name}")

            table_exists_query_table_name = table_name_in_db.replace("_", "\\_")
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

                source_columns = df_to_write.columns
                
                update_set_parts = [f"target.`{col_name}` = source.`{col_name}`" for col_name in source_columns]
                update_set_clause = ", ".join(update_set_parts)

                insert_columns = ", ".join([f"`{col_name}`" for col_name in source_columns])
                insert_values = ", ".join([f"source.`{col_name}`" for col_name in source_columns])

                merge_sql = f"""
                MERGE INTO {full_table_name} AS target
                USING {temp_view_name} AS source
                ON {merge_condition}
                WHEN MATCHED THEN
                    UPDATE SET {update_set_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns}) VALUES ({insert_values})
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
            total_write_duration_accumulator += (time.time() - write_start_time_local)

    transform_stage_duration_val = time.time() - transform_stage_start_time
    g_transform_stage_duration.set(transform_stage_duration_val)
    print(f"--- HOÀN THÀNH GIAI ĐOẠN TRANSFORM TRONG {transform_stage_duration_val:.2f} giây ---")
    
    print("\n--- Bắt đầu ghi dữ liệu vào các bảng CLEAN sử dụng MERGE ---")

    write_iceberg_table_with_merge(authors_df, "authors", primary_key_cols=["AuthorID"])
    authors_df.unpersist(); print("Đã giải phóng cache cho: authors_df")

    write_iceberg_table_with_merge(topics_df, "topics", primary_key_cols=["TopicID"])

    write_iceberg_table_with_merge(subtopics_df, "subtopics", primary_key_cols=["SubTopicID"])

    write_iceberg_table_with_merge(keywords_dim_df, "keywords", primary_key_cols=["KeywordID"])
    keywords_dim_df.unpersist(); print("Đã giải phóng cache cho: keywords_dim_df")

    write_iceberg_table_with_merge(references_dim_df, "references_table", primary_key_cols=["ReferenceID"])
    references_dim_df.unpersist(); print("Đã giải phóng cache cho: references_dim_df")
    
    if count_articles_to_write > 0:
        write_iceberg_table_with_merge(articles_to_write_df, "articles",
                                       primary_key_cols=["ArticleID"],
                                       partition_cols=["PublicationDate"])
    else:
        print("Không có dữ liệu articles để ghi.")

    topics_df.unpersist(); print("Đã giải phóng cache cho: topics_df")
    
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
    traceback.print_exc()
    job_succeeded_flag = False
finally:
    overall_job_duration = time.time() - overall_job_start_time
    g_job_duration.set(overall_job_duration)

    if job_succeeded_flag:
        g_job_status.set(1)
        g_job_last_success_ts.set(int(time.time()))
    else:
        g_job_status.set(0)

    g_total_write_duration.set(total_write_duration_accumulator)

    if actual_start_date_for_metric:
        g_processed_date_start_ts.set(int(actual_start_date_for_metric.timestamp()))
    if actual_end_date_for_metric:
        g_processed_date_end_ts.set(int(actual_end_date_for_metric.timestamp()))

    push_metrics_to_gateway()

    if 'spark' in locals() and spark:
        if 'articles_to_write_df' in locals() and \
         hasattr(articles_to_write_df, 'storageLevel') and \
         articles_to_write_df.storageLevel.useMemory:
                articles_to_write_df.unpersist()
                print("Đã unpersist articles_to_write_df.")
        spark.stop()
        print("Spark session đã được dừng.")
