from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, expr, date_format, dayofmonth, month, year, quarter
from pyspark.sql.types import IntegerType 

minio_endpoint = "http://minio1:9000"
minio_access_key = "rH4arFLYBxl55rh2zmN1"
minio_secret_key = "AtEB2XiMAznxvJXRvaXxigdIewIMVKscgPg7dJjI"
nessie_uri = "http://nessie:19120/api/v2"
nessie_default_branch = "main"

curated_catalog_name = "nessie-curated-news"
curated_catalog_warehouse_path = "s3a://curated-news-lakehouse/nessie_curated_news_warehouse"
CURATED_DATABASE_NAME = "news_curated_db"

app_name = "PopulateDimDate"

spark_builder = SparkSession.builder.appName(app_name)

spark_builder = spark_builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark_builder = spark_builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")

spark_builder = spark_builder.config(f"spark.sql.catalog.{curated_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{curated_catalog_name}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config(f"spark.sql.catalog.{curated_catalog_name}.uri", nessie_uri) \
    .config(f"spark.sql.catalog.{curated_catalog_name}.ref", nessie_default_branch) \
    .config(f"spark.sql.catalog.{curated_catalog_name}.warehouse", curated_catalog_warehouse_path) \
    .config(f"spark.sql.catalog.{curated_catalog_name}.authentication.type", "NONE")

spark = spark_builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("SparkSession đã được khởi tạo để tạo DimDate.")

start_date_str = "2015-01-01"
end_date_str = "2029-12-31"

dim_date_table_name = "dim_date"
full_dim_date_table_path = f"`{curated_catalog_name}`.`{CURATED_DATABASE_NAME}`.`{dim_date_table_name}`"

print(f"Sẽ tạo DimDate từ {start_date_str} đến {end_date_str} vào bảng {full_dim_date_table_path}")

try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{curated_catalog_name}`.`{CURATED_DATABASE_NAME}`")
    print(f"Database `{curated_catalog_name}`.`{CURATED_DATABASE_NAME}` đã được kiểm tra/tạo.")
    
    dim_date_df = spark.sql(f"SELECT sequence(to_date('{start_date_str}'), to_date('{end_date_str}'), interval 1 day) as date_array") \
        .select(explode(col("date_array")).alias("FullDateAlternateKey"))

    dim_date_df = dim_date_df \
        .withColumn("DateKey", expr("cast(date_format(FullDateAlternateKey, 'yyyyMMdd') as int)")) \
        .withColumn("DayNameOfWeek", date_format(col("FullDateAlternateKey"), "EEEE")) \
        .withColumn("DayNumberOfMonth", dayofmonth(col("FullDateAlternateKey"))) \
        .withColumn("DayNumberOfYear", date_format(col("FullDateAlternateKey"), "D").cast(IntegerType())) \
        .withColumn("MonthName", date_format(col("FullDateAlternateKey"), "MMMM")) \
        .withColumn("MonthNumberOfYear", month(col("FullDateAlternateKey"))) \
        .withColumn("CalendarQuarter", quarter(col("FullDateAlternateKey"))) \
        .withColumn("CalendarYear", year(col("FullDateAlternateKey"))) \
        .select(
            "DateKey",
            "FullDateAlternateKey",
            "DayNameOfWeek",
            "DayNumberOfMonth",
            "DayNumberOfYear",
            "MonthName",
            "MonthNumberOfYear",
            "CalendarQuarter",
            "CalendarYear"
        )

    print(f"\n--- 5 dòng đầu của DimDate sẽ được tạo ---")
    dim_date_df.show(5)
    print(f"Tổng số ngày sẽ được tạo trong DimDate: {dim_date_df.count()}")

    print(f"\nĐang ghi DimDate vào bảng {full_dim_date_table_path}...")
    dim_date_df.writeTo(full_dim_date_table_path) \
        .tableProperty("write.format.default", "parquet") \
        .createOrReplace()
    
    print(f"Bảng {full_dim_date_table_path} đã được tạo/cập nhật thành công.")

    print(f"\n--- 5 dòng đầu của DimDate từ bảng Iceberg ---")
    spark.read.format("iceberg").load(full_dim_date_table_path).show(5)

except Exception as e:
    print(f"Lỗi trong quá trình tạo DimDate: {e}")
    raise e

finally:
    print("\nHoàn tất script tạo DimDate.")
    spark.stop()
