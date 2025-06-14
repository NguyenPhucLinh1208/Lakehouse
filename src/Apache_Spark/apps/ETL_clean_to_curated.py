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
import argparse
import time
from dotenv import load_dotenv
import os
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
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

curated_catalog_name = "nessie-curated-news"
curated_catalog_warehouse_path = "s3a://curated-news-lakehouse/nessie_curated_news_warehouse"
CURATED_DATABASE_NAME = "news_curated_db"

app_name = "NewsETLCleanToCurated"

VIETNAM_TZ = pytz.timezone('Asia/Ho_Chi_Minh')
PYTHON_DATE_FORMAT_PATTERN = "%Y-%m-%d"
SPARK_SQL_DATE_FORMAT_PATTERN = "yyyy-MM-dd"

arg_parser = argparse.ArgumentParser(description="Tham số cho ngày bắt đầu và kết thúc cho ETL")
arg_parser.add_argument(
    "--etl-start-date",
    type=str,
    default=None,
    help=f"Start date ({PYTHON_DATE_FORMAT_PATTERN}). Mặc định (last 7 days, giờ VN)."
)
arg_parser.add_argument(
    "--etl-end-date",
    type=str,
    default=None,
    help=f"End date ({PYTHON_DATE_FORMAT_PATTERN}). Mặc định (last 7 days, giờ VN)."
)
arg_parser.add_argument(
    "--airflow-run-id",
    type=str,
    default=f"run_clean_curated_{datetime.now().strftime('%Y%m%d%H%M%S')}",
    help="Airflow Run ID hoặc một instance ID duy nhất cho lần chạy job."
)
args = arg_parser.parse_args()

ETL_START_DATE_STR = args.etl_start_date
ETL_END_DATE_STR = args.etl_end_date

PROMETHEUS_PUSHGATEWAY_URL = "http://pushgateway:9091"
JOB_NAME = app_name
INSTANCE_ID = args.airflow_run_id

registry = CollectorRegistry()
METRIC_PREFIX = "etl"

g_job_duration = Gauge(f'{METRIC_PREFIX}_job_duration_seconds', 'Tổng thời gian chạy của job ETL (CleanToCurated)', registry=registry)
g_job_status = Gauge(f'{METRIC_PREFIX}_job_status', 'Trạng thái kết thúc job (CleanToCurated) (1=thành công, 0=thất bại)', registry=registry)
g_job_last_success_ts = Gauge(f'{METRIC_PREFIX}_job_last_success_timestamp', 'Unix timestamp của lần chạy job (CleanToCurated) thành công cuối cùng', registry=registry)
g_processed_date_start_ts = Gauge(f'{METRIC_PREFIX}_job_processed_date_range_start_timestamp', 'Unix timestamp của ngày bắt đầu xử lý (0h giờ VN)', registry=registry)
g_processed_date_end_ts = Gauge(f'{METRIC_PREFIX}_job_processed_date_range_end_timestamp', 'Unix timestamp của ngày kết thúc xử lý (0h giờ VN)', registry=registry)
g_valid_url_records_count = Gauge(f'{METRIC_PREFIX}_valid_url_records_processed_count', 'Số lượng bản ghi input articles (từ Clean) được xử lý', registry=registry)
g_read_stage_duration = Gauge(f'{METRIC_PREFIX}_read_stage_duration_seconds', 'Thời gian đọc dữ liệu từ Clean zone', registry=registry)
g_articles_records_to_write = Gauge(f'{METRIC_PREFIX}_articles_records_to_write_count', 'Số lượng bản ghi fact_article_publication chuẩn bị để ghi', registry=registry)
g_total_write_duration = Gauge(f'{METRIC_PREFIX}_total_write_duration_seconds', 'Tổng thời gian ghi/merge tất cả các bảng', registry=registry)
g_transform_stage_duration = Gauge(f'{METRIC_PREFIX}_transform_stage_duration_seconds', 'Tổng thời gian cho giai đoạn transform dữ liệu', registry=registry)
g_articles_by_pub_date_count = Gauge(
    f'{METRIC_PREFIX}_articles_by_publication_date_count',
    'Số lượng bài báo (fact_article_publication) theo ngày xuất bản (giờ VN)',
    ['publication_date'],
    registry=registry
)
g_articles_by_topic_count = Gauge(
    f'{METRIC_PREFIX}_articles_by_topic_count',
    'Số lượng bài báo (fact_article_publication) theo chủ đề',
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

def unpersist_df(df_name):
    if df_name in cached_dataframes:
        df = cached_dataframes.pop(df_name)
        if df:
            try:
                df.unpersist()
                print(f"Đã giải phóng cache cho DataFrame: {df_name}")
            except Exception as e:
                print(f"Lỗi khi giải phóng cache cho {df_name}: {e}")

actual_start_date_process_for_metric = None
actual_end_date_process_for_metric = None
overall_job_start_time = time.time()
job_succeeded_flag = False
spark = None
total_write_duration_accumulator = 0.0

try:
    read_duration_val = 0.0
    spark_builder = SparkSession.builder.appName(app_name)
    spark_builder = spark_builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") \
        .config("spark.sql.shuffle.partitions", "4")

    spark_builder = spark_builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")

    spark_builder = spark_builder.config(f"spark.sql.catalog.{clean_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{clean_catalog_name}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config(f"spark.sql.catalog.{clean_catalog_name}.uri", nessie_uri) \
        .config(f"spark.sql.catalog.{clean_catalog_name}.ref", nessie_default_branch) \
        .config(f"spark.sql.catalog.{clean_catalog_name}.warehouse", clean_catalog_warehouse_path) \
        .config(f"spark.sql.catalog.{clean_catalog_name}.authentication.type", "NONE")

    spark_builder = spark_builder.config(f"spark.sql.catalog.{curated_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{curated_catalog_name}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config(f"spark.sql.catalog.{curated_catalog_name}.uri", nessie_uri) \
        .config(f"spark.sql.catalog.{curated_catalog_name}.ref", nessie_default_branch) \
        .config(f"spark.sql.catalog.{curated_catalog_name}.warehouse", curated_catalog_warehouse_path) \
        .config(f"spark.sql.catalog.{curated_catalog_name}.authentication.type", "NONE")

    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"SparkSession đã được khởi tạo cho: {app_name} với múi giờ session: {spark.conf.get('spark.sql.session.timeZone')}.")

    def get_date_range(start_date_str=None, end_date_str=None, default_days=7):
        global actual_start_date_process_for_metric, actual_end_date_process_for_metric
        calculated_start_date_aware, calculated_end_date_aware = None, None
        
        if start_date_str and end_date_str:
            try:
                start_date_naive = datetime.strptime(start_date_str, PYTHON_DATE_FORMAT_PATTERN)
                end_date_naive = datetime.strptime(end_date_str, PYTHON_DATE_FORMAT_PATTERN)
                calculated_start_date_aware = VIETNAM_TZ.localize(start_date_naive)
                calculated_end_date_aware = VIETNAM_TZ.localize(end_date_naive)
                print(f"Sử dụng khoảng thời gian tùy chỉnh (giờ VN): {calculated_start_date_aware.strftime(PYTHON_DATE_FORMAT_PATTERN)} đến {calculated_end_date_aware.strftime(PYTHON_DATE_FORMAT_PATTERN)}")
            except ValueError:
                print(f"Lỗi định dạng ngày: '{start_date_str}', '{end_date_str}'. Vui lòng dùng định dạng '{PYTHON_DATE_FORMAT_PATTERN}'.")
                g_job_status.set(0)
                raise
        else:
            now_in_vietnam = datetime.now(VIETNAM_TZ)
            calculated_end_date_aware = (now_in_vietnam - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            calculated_start_date_aware = calculated_end_date_aware - timedelta(days=default_days - 1)
            print(f"Sử dụng khoảng thời gian mặc định (giờ VN): {default_days} ngày, từ {calculated_start_date_aware.strftime(PYTHON_DATE_FORMAT_PATTERN)} đến {calculated_end_date_aware.strftime(PYTHON_DATE_FORMAT_PATTERN)}")
        
        actual_start_date_process_for_metric = calculated_start_date_aware
        actual_end_date_process_for_metric = calculated_end_date_aware
        return calculated_start_date_aware, calculated_end_date_aware

    cached_dataframes = {}
    def read_iceberg_table_with_cache(table_name, catalog_name, db_name, date_filter_col=None, start_date=None, end_date=None, use_cache=False):
        full_table_name_key = f"{catalog_name}.{db_name}.{table_name}"
        if use_cache and full_table_name_key in cached_dataframes:
            print(f"Đang sử dụng cache cho bảng: {full_table_name_key}")
            return cached_dataframes[full_table_name_key]
        
        full_table_name = f"`{catalog_name}`.`{db_name}`.`{table_name}`"
        print(f"Đang đọc từ bảng: {full_table_name}")
        try:
            df = spark.read.format("iceberg").load(full_table_name)
            if date_filter_col and start_date and end_date:
                start_date_str_for_filter = start_date.strftime(PYTHON_DATE_FORMAT_PATTERN)
                end_date_str_for_filter = end_date.strftime(PYTHON_DATE_FORMAT_PATTERN)
                print(f"Áp dụng bộ lọc ngày trên cột '{date_filter_col}': từ {start_date_str_for_filter} đến {end_date_str_for_filter} (giờ VN)")
                df = df.filter(col(date_filter_col) >= lit(start_date_str_for_filter).cast(DateType())) \
                       .filter(col(date_filter_col) <= lit(end_date_str_for_filter).cast(DateType()))
            
            if use_cache:
                df = df.cache()
                cached_dataframes[full_table_name_key] = df
                print(f"Đã cache bảng: {full_table_name_key}")
            return df
        except Exception as e:
            print(f"Lỗi khi đọc bảng {full_table_name}: {e}")
            g_job_status.set(0)
            raise

    def write_curated_iceberg_table(df_to_write, table_name, write_mode,
                                     primary_key_cols_for_merge=None,
                                     partition_cols=None,
                                     catalog_name=curated_catalog_name, db_name=CURATED_DATABASE_NAME):
        
        global total_write_duration_accumulator
        
        full_table_name = f"`{catalog_name}`.`{db_name}`.`{table_name}`"
        temp_view_name = f"{table_name}_source_view_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

        if df_to_write.isEmpty():
            print(f"Không có dữ liệu để ghi vào bảng {full_table_name} với chế độ '{write_mode}'. Bỏ qua.")
            return

        write_start_time_local = time.time()

        try:
            print(f"Chuẩn bị ghi vào bảng Curated: {full_table_name} (Mode: {write_mode})")
            df_to_write.createOrReplaceTempView(temp_view_name)

            table_exists_query_table_name = table_name.split('.')[-1].replace("_", "\\_")
            table_exists = spark.sql(f"SHOW TABLES IN `{catalog_name}`.`{db_name}` LIKE '{table_exists_query_table_name}'").count() > 0
            
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
            g_job_status.set(0)
            raise
        finally:
            spark.catalog.dropTempView(temp_view_name)
            total_write_duration_accumulator += (time.time() - write_start_time_local)


    start_date_process, end_date_process = get_date_range(ETL_START_DATE_STR, ETL_END_DATE_STR)

    read_clean_stage_start_time = time.time()
    print("\n--- Bước 1: Đọc dữ liệu từ vùng CLEAN ---")
    authors_clean_df = read_iceberg_table_with_cache("authors", clean_catalog_name, CLEAN_DATABASE_NAME, use_cache=True)
    topics_clean_df = read_iceberg_table_with_cache("topics", clean_catalog_name, CLEAN_DATABASE_NAME, use_cache=True)
    subtopics_clean_df = read_iceberg_table_with_cache("subtopics", clean_catalog_name, CLEAN_DATABASE_NAME, use_cache=True)
    keywords_clean_df = read_iceberg_table_with_cache("keywords", clean_catalog_name, CLEAN_DATABASE_NAME, use_cache=True)
    references_clean_df = read_iceberg_table_with_cache("references_table", clean_catalog_name, CLEAN_DATABASE_NAME, use_cache=True)
    base_comment_interactions_clean_df = read_iceberg_table_with_cache("comment_interactions", clean_catalog_name, CLEAN_DATABASE_NAME, use_cache=True)
    articles_clean_df = read_iceberg_table_with_cache("articles", clean_catalog_name, CLEAN_DATABASE_NAME,
                                                      date_filter_col="PublicationDate",
                                                      start_date=start_date_process, end_date=end_date_process, use_cache=True)
    articles_clean_df = articles_clean_df.coalesce(4)
    
    count_input_articles = 0
    if not articles_clean_df.isEmpty():
        count_input_articles = articles_clean_df.count()
    g_valid_url_records_count.set(count_input_articles)
    print(f"Đã set metric 'g_valid_url_records_count' (số bài báo đầu vào từ Clean) = {count_input_articles}")

    read_duration_val = time.time() - read_clean_stage_start_time
    g_read_stage_duration.set(read_duration_val)

    if count_input_articles == 0:
        print(f"Không có bài báo nào trong khoảng từ {start_date_process.strftime(PYTHON_DATE_FORMAT_PATTERN)} đến {end_date_process.strftime(PYTHON_DATE_FORMAT_PATTERN)}. Dừng ETL.")
        job_succeeded_flag = True
        g_articles_records_to_write.set(0)
    else:
        processed_article_ids_df = articles_clean_df.select("ArticleID").distinct()
        
        article_keywords_clean_df = read_iceberg_table_with_cache("article_keywords", clean_catalog_name, CLEAN_DATABASE_NAME, use_cache=True) \
            .join(processed_article_ids_df, ["ArticleID"], "inner")
        article_references_clean_df = read_iceberg_table_with_cache("article_references", clean_catalog_name, CLEAN_DATABASE_NAME, use_cache=True) \
            .join(processed_article_ids_df, ["ArticleID"], "inner")
        comments_clean_df = read_iceberg_table_with_cache("comments", clean_catalog_name, CLEAN_DATABASE_NAME, use_cache=True) \
            .join(processed_article_ids_df, ["ArticleID"], "inner")

        processed_comment_ids_df = comments_clean_df.select("CommentID").distinct()
        comment_interactions_for_facts_df = base_comment_interactions_clean_df.join(processed_comment_ids_df, ["CommentID"], "inner")
    
        print(f"Đã hoàn thành Bước 1 - Đọc dữ liệu từ Clean. Thời gian: {read_duration_val:.2f}s")

        print("\n--- Bước 2: Xây dựng Dimension Tables cho CURATED ---")
        
        print("Đang đọc dim_date (đã được populate trước đó)...")
        dim_date_df = read_iceberg_table_with_cache("dim_date", curated_catalog_name, CURATED_DATABASE_NAME, use_cache=True)
        if dim_date_df.isEmpty():
            print("LỖI NGHIÊM TRỌNG: Bảng dim_date rỗng hoặc không tồn tại. Dừng ETL.")
            g_job_status.set(0)
            raise SystemExit("Dừng job vì dim_date rỗng.")

        print("Đang xử lý dim_author...")
        dim_author_df_to_write = authors_clean_df \
            .withColumn("AuthorKey", xxhash64(col("AuthorID"))) \
            .select(col("AuthorKey"), col("AuthorID").alias("AuthorID_NK"), col("AuthorName"))
        write_curated_iceberg_table(dim_author_df_to_write, "dim_author", write_mode="merge", primary_key_cols_for_merge=["AuthorID_NK"])
        dim_author_df = read_iceberg_table_with_cache("dim_author", curated_catalog_name, CURATED_DATABASE_NAME, use_cache=True).alias("dim_author_cached")

        print("Đang xử lý dim_topic...")
        dim_topic_df_to_write = topics_clean_df \
            .withColumn("TopicKey", xxhash64(col("TopicID"))) \
            .select(col("TopicKey"), col("TopicID").alias("TopicID_NK"), col("TopicName"))
        write_curated_iceberg_table(dim_topic_df_to_write, "dim_topic", write_mode="merge", primary_key_cols_for_merge=["TopicID_NK"])
        dim_topic_df = read_iceberg_table_with_cache("dim_topic", curated_catalog_name, CURATED_DATABASE_NAME, use_cache=True).alias("dim_topic_cached")

        print("Đang xử lý dim_sub_topic...")
        dim_subtopic_df_to_write = subtopics_clean_df \
            .join(dim_topic_df, subtopics_clean_df.TopicID == dim_topic_df.TopicID_NK, "left_outer") \
            .withColumn("SubTopicKey", xxhash64(col("SubTopicID"))) \
            .select(
                col("SubTopicKey"),
                col("SubTopicID").alias("SubTopicID_NK"),
                col("SubTopicName"),
                dim_topic_df.TopicKey.alias("ParentTopicKey"),
                dim_topic_df.TopicName.alias("ParentTopicName")
            )
        write_curated_iceberg_table(dim_subtopic_df_to_write, "dim_sub_topic", write_mode="merge", primary_key_cols_for_merge=["SubTopicID_NK"])
        dim_subtopic_df = read_iceberg_table_with_cache("dim_sub_topic", curated_catalog_name, CURATED_DATABASE_NAME, use_cache=True).alias("dim_subtopic_cached")

        print("Đang xử lý dim_keyword...")
        dim_keyword_df_to_write = keywords_clean_df \
            .withColumn("KeywordKey", xxhash64(col("KeywordID"))) \
            .select(col("KeywordKey"), col("KeywordID").alias("KeywordID_NK"), col("KeywordText"))
        write_curated_iceberg_table(dim_keyword_df_to_write, "dim_keyword", write_mode="merge", primary_key_cols_for_merge=["KeywordID_NK"])
        dim_keyword_df = read_iceberg_table_with_cache("dim_keyword", curated_catalog_name, CURATED_DATABASE_NAME, use_cache=True).alias("dim_keyword_cached")

        print("Đang xử lý dim_reference_source...")
        dim_referencesource_df_to_write = references_clean_df \
            .withColumn("ReferenceSourceKey", xxhash64(col("ReferenceID"))) \
            .select(col("ReferenceSourceKey"), col("ReferenceID").alias("ReferenceID_NK"), col("ReferenceText"))
        write_curated_iceberg_table(dim_referencesource_df_to_write, "dim_reference_source", write_mode="merge", primary_key_cols_for_merge=["ReferenceID_NK"])
        dim_referencesource_df = read_iceberg_table_with_cache("dim_reference_source", curated_catalog_name, CURATED_DATABASE_NAME, use_cache=True).alias("dim_referencesource_cached")

        print("Đang xử lý dim_interaction_type...")
        dim_interactiontype_df_to_write = spark.createDataFrame([], StructType([StructField("InteractionTypeKey", LongType(), True), StructField("InteractionTypeName", StringType(), True), StructField("LastUpdatedTimestamp_Curated", TimestampType(), True)]))
        if not base_comment_interactions_clean_df.isEmpty() and "InteractionType" in base_comment_interactions_clean_df.columns:
            dim_interactiontype_df_to_write = base_comment_interactions_clean_df \
                .select(col("InteractionType").alias("InteractionTypeName")) \
                .filter(col("InteractionTypeName").isNotNull()) \
                .distinct() \
                .withColumn("InteractionTypeKey", xxhash64(col("InteractionTypeName"))) \
                .select("InteractionTypeKey", "InteractionTypeName")
        else:
            print("Không có dữ liệu InteractionType từ base_comment_interactions_clean_df hoặc cột không tồn tại.")

        write_curated_iceberg_table(dim_interactiontype_df_to_write, "dim_interaction_type", write_mode="merge", primary_key_cols_for_merge=["InteractionTypeName"])
        dim_interactiontype_df = read_iceberg_table_with_cache("dim_interaction_type", curated_catalog_name, CURATED_DATABASE_NAME, use_cache=True).alias("dim_interactiontype_cached")

        if count_input_articles > 0:
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
                .join(dim_author_df, col("AuthorID_lookup") == col("dim_author_cached.AuthorID_NK"), "left_outer") \
                .join(dim_topic_df, col("TopicID_lookup") == col("dim_topic_cached.TopicID_NK"), "left_outer") \
                .join(dim_subtopic_df, col("SubTopicID_lookup") == col("dim_subtopic_cached.SubTopicID_NK"), "left_outer") \
                .select(
                    dim_date_df.DateKey.alias("PublicationDateKey"),
                    col("ArticlePublicationTimestamp"),
                    col("dim_author_cached.AuthorKey"),
                    col("dim_topic_cached.TopicKey"),
                    col("dim_subtopic_cached.SubTopicKey"),
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
            
            fact_article_publication_df.cache()
            count_fact_articles_to_write = fact_article_publication_df.count()

            unpersist_df(f"{clean_catalog_name}.{CLEAN_DATABASE_NAME}.articles")
            unpersist_df(f"{curated_catalog_name}.{CURATED_DATABASE_NAME}.dim_date")
            unpersist_df(f"{curated_catalog_name}.{CURATED_DATABASE_NAME}.dim_author")
            unpersist_df(f"{curated_catalog_name}.{CURATED_DATABASE_NAME}.dim_sub_topic")

            if count_fact_articles_to_write > 0:
                g_articles_records_to_write.set(count_fact_articles_to_write)
                print(f"Đã set metric 'g_articles_records_to_write_count' = {count_fact_articles_to_write}")
                
                print("Đang tính toán số lượng bài báo theo ngày xuất bản (fact)...")
                articles_by_date_collected = fact_article_publication_df \
                    .withColumn("PublicationDateString", date_format(col("ArticlePublicationTimestamp"), SPARK_SQL_DATE_FORMAT_PATTERN)) \
                    .groupBy("PublicationDateString") \
                    .count() \
                    .collect()
                for row in articles_by_date_collected:
                    if row["PublicationDateString"]:
                        g_articles_by_pub_date_count.labels(publication_date=row["PublicationDateString"]).set(row["count"])
                print("Đã set metric số lượng bài báo theo ngày xuất bản (fact).")

                print("Đang tính toán số lượng bài báo theo chủ đề (fact)...")
                
                projected_topics_for_join = dim_topic_df.select(
                    col("dim_topic_cached.TopicKey"),
                    col("dim_topic_cached.TopicName").alias("unique_topic_name_for_grouping")
                )

                articles_by_topic_collected = fact_article_publication_df \
                    .join(projected_topics_for_join, fact_article_publication_df.TopicKey == projected_topics_for_join.TopicKey, "inner") \
                    .groupBy("unique_topic_name_for_grouping") \
                    .count() \
                    .collect()

                for row in articles_by_topic_collected:
                    if row["unique_topic_name_for_grouping"]:
                        g_articles_by_topic_count.labels(topic_name=row["unique_topic_name_for_grouping"]).set(row["count"])
                print("Đã set metric số lượng bài báo theo chủ đề (fact).")

                unpersist_df(f"{curated_catalog_name}.{CURATED_DATABASE_NAME}.dim_topic")
                
                write_curated_iceberg_table(fact_article_publication_df, "fact_article_publication",
                                            write_mode="overwrite_dynamic_partitions",
                                            partition_cols=["ArticlePublicationTimestamp"])
            else:
                print("Không có dữ liệu fact_article_publication để ghi hoặc tính metrics chi tiết.")
                g_articles_records_to_write.set(0)

            fact_pub_keys_df = fact_article_publication_df.select(
                "ArticleID_NK", 
                col("PublicationDateKey").alias("ArticlePublicationDateKey"),
                "AuthorKey", "TopicKey", "SubTopicKey"
            ).cache()
            fact_pub_keys_df.count()

            fact_article_publication_df.unpersist()
            print("Đã giải phóng cache cho DataFrame: fact_article_publication_df")

            print("Đang xử lý fact_article_keyword...")
            fact_article_keyword_df = article_keywords_clean_df \
                .join(fact_pub_keys_df.alias("fpk"), article_keywords_clean_df.ArticleID == col("fpk.ArticleID_NK"), "inner") \
                .join(dim_keyword_df, article_keywords_clean_df.KeywordID == col("dim_keyword_cached.KeywordID_NK"), "left_outer") \
                .select(
                    col("fpk.ArticlePublicationDateKey"),
                    col("fpk.ArticleID_NK"),
                    col("dim_keyword_cached.KeywordKey"),
                    col("fpk.AuthorKey"),
                    col("fpk.TopicKey"),
                    col("fpk.SubTopicKey"),
                    lit(1).alias("IsKeywordTaggedToArticle")
                ).na.fill(-1, subset=["ArticlePublicationDateKey", "KeywordKey", "AuthorKey", "TopicKey", "SubTopicKey"])
            if not fact_article_keyword_df.isEmpty():
                write_curated_iceberg_table(fact_article_keyword_df, "fact_article_keyword",
                                            write_mode="overwrite_dynamic_partitions",
                                            partition_cols=["ArticlePublicationDateKey"])
            else:
                print("Không có dữ liệu cho fact_article_keyword.")
            
            unpersist_df(f"{clean_catalog_name}.{CLEAN_DATABASE_NAME}.keywords")
            unpersist_df(f"{curated_catalog_name}.{CURATED_DATABASE_NAME}.dim_keyword")

            print("Đang xử lý fact_article_reference...")
            fact_article_reference_df = article_references_clean_df \
                .join(fact_pub_keys_df.alias("fpk"), article_references_clean_df.ArticleID == col("fpk.ArticleID_NK"), "inner") \
                .join(dim_referencesource_df, article_references_clean_df.ReferenceID == col("dim_referencesource_cached.ReferenceID_NK"), "left_outer") \
                .select(
                    col("fpk.ArticlePublicationDateKey"),
                    col("fpk.ArticleID_NK"),
                    col("dim_referencesource_cached.ReferenceSourceKey"),
                    col("fpk.AuthorKey"),
                    col("fpk.TopicKey"),
                    col("fpk.SubTopicKey"),
                    lit(1).alias("IsReferenceUsedInArticle")
                ).na.fill(-1, subset=["ArticlePublicationDateKey", "ReferenceSourceKey", "AuthorKey", "TopicKey", "SubTopicKey"])
            if not fact_article_reference_df.isEmpty():
                write_curated_iceberg_table(fact_article_reference_df, "fact_article_reference",
                                            write_mode="overwrite_dynamic_partitions",
                                            partition_cols=["ArticlePublicationDateKey"])
            else:
                print("Không có dữ liệu cho fact_article_reference.")
            
            unpersist_df(f"{clean_catalog_name}.{CLEAN_DATABASE_NAME}.references_table")
            unpersist_df(f"{curated_catalog_name}.{CURATED_DATABASE_NAME}.dim_reference_source")

            print("Đang xử lý fact_top_comment_activity...")
            fact_top_comment_activity_df = comments_clean_df \
                .join(fact_pub_keys_df.alias("fpk"), comments_clean_df.ArticleID == col("fpk.ArticleID_NK"), "inner") \
                .select(
                    col("fpk.ArticlePublicationDateKey"),
                    col("fpk.ArticlePublicationDateKey").alias("CommentDateKey"),
                    col("fpk.ArticleID_NK"),
                    comments_clean_df.CommentID.alias("CommentID_NK"),
                    col("fpk.AuthorKey"),
                    col("fpk.TopicKey"),
                    col("fpk.SubTopicKey"),
                    col("CommenterName"),
                    lit(1).alias("IsTopComment"),
                    col("TotalLikes").alias("LikesOnTopComment")
                ).na.fill({"ArticlePublicationDateKey": -1, "CommentDateKey": -1, "AuthorKey": -1, "TopicKey": -1, "SubTopicKey": -1})
            if not fact_top_comment_activity_df.isEmpty():
                write_curated_iceberg_table(fact_top_comment_activity_df, "fact_top_comment_activity",
                                            write_mode="overwrite_dynamic_partitions",
                                            partition_cols=["ArticlePublicationDateKey"])
            else:
                print("Không có dữ liệu cho fact_top_comment_activity.")

            print("Đang xử lý fact_top_comment_interaction_detail...")
            fact_top_comment_interaction_detail_df = comment_interactions_for_facts_df.alias("cif") \
                .join(comments_clean_df.select("CommentID", "ArticleID").alias("cmt"), 
                      col("cif.CommentID") == col("cmt.CommentID"), "inner") \
                .join(fact_pub_keys_df.alias("fpk"), 
                      col("cmt.ArticleID") == col("fpk.ArticleID_NK"), "inner") \
                .join(dim_interactiontype_df.alias("dit"), 
                      col("cif.InteractionType") == col("dit.InteractionTypeName"), "left_outer") \
                .select(
                    col("fpk.ArticlePublicationDateKey").alias("ArticlePublicationDateKey"),
                    col("fpk.ArticlePublicationDateKey").alias("InteractionDateKey"),
                    col("cmt.ArticleID").alias("ArticleID_NK"),
                    col("cif.CommentID").alias("CommentID_NK"),
                    col("dit.InteractionTypeKey"),
                    col("fpk.AuthorKey"),
                    col("fpk.TopicKey"),
                    col("fpk.SubTopicKey"),
                    lit(1).alias("InteractionInstanceCount"),
                    col("cif.InteractionCount").alias("InteractionValue")
                ).na.fill({
                    "ArticlePublicationDateKey": -1, "InteractionDateKey": -1, "InteractionTypeKey": -1,
                    "AuthorKey": -1, "TopicKey": -1, "SubTopicKey": -1
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
            
            fact_pub_keys_df.unpersist()
            print("Đã giải phóng cache cho DataFrame: fact_pub_keys_df")
            unpersist_df(f"{curated_catalog_name}.{CURATED_DATABASE_NAME}.dim_interaction_type")

        else:
            print("Bỏ qua xây dựng Fact tables vì không có dữ liệu articles đầu vào.")
            g_articles_records_to_write.set(0)

        print(f"\n--- Hoàn thành quy trình ETL từ Clean sang Curated cho khoảng ngày {start_date_process.strftime(PYTHON_DATE_FORMAT_PATTERN)} đến {end_date_process.strftime(PYTHON_DATE_FORMAT_PATTERN)} ---")
        job_succeeded_flag = True

except SystemExit as se:
    print(f"Job dừng chủ động: {se}")
except ValueError as ve:
    print(f"LỖI VALUE ERROR (ví dụ: định dạng ngày): {ve}")
    job_succeeded_flag = False
except Exception as e:
    print(f"LỖI CHÍNH TRONG QUÁ TRÌNH ETL: {e}")
    import traceback
    traceback.print_exc()
    job_succeeded_flag = False
    if 'g_job_status' in globals(): g_job_status.set(0)
finally:
    overall_job_duration_val = time.time() - overall_job_start_time
    g_job_duration.set(overall_job_duration_val)
    print(f"Tổng thời gian chạy job: {overall_job_duration_val:.2f} giây.")

    if job_succeeded_flag:
        g_job_status.set(1)
        g_job_last_success_ts.set(int(time.time()))
    else:
        g_job_status.set(0)
    
    g_total_write_duration.set(total_write_duration_accumulator)
    
    transform_duration = overall_job_duration_val - read_duration_val - total_write_duration_accumulator
    g_transform_stage_duration.set(max(0, transform_duration))

    if actual_start_date_process_for_metric:
        g_processed_date_start_ts.set(int(actual_start_date_process_for_metric.timestamp()))
    if actual_end_date_process_for_metric:
        g_processed_date_end_ts.set(int(actual_end_date_process_for_metric.timestamp()))

    push_metrics_to_gateway()

    if 'spark' in locals() and spark:
        print("Dọn dẹp các cache còn sót lại...")
        for df_name_key in list(cached_dataframes.keys()):
            unpersist_df(df_name_key)
        
        spark.stop()
        print("Spark session đã được dừng.")
    else:
        print("Không có Spark session để dừng hoặc đã dừng trước đó.")
