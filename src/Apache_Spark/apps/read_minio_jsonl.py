from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

def main():
    minio_endpoint = "http://minio1:9000"
    minio_access_key = "rH4arFLYBxl55rh2zmN1"
    minio_secret_key = "AtEB2XiMAznxvJXRvaXxigdIewIMVKscgPg7dJjI"
    minio_bucket_name = "raw-news-lakehouse"
    file_path_in_bucket = "2024/01/01/Bất_động_sản.jsonl"

    s3_path = f"s3a://{minio_bucket_name}/{file_path_in_bucket}"

    spark = SparkSession.builder \
        .appName("MinIODataReaderWithCheck") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.json.lines", "true") \
        .getOrCreate()

    print(f"SparkSession đã khởi tạo. Ứng dụng: {spark.conf.get('spark.app.name')}")
    print(f"Đang kiểm tra đường dẫn MinIO: {s3_path}")

    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        uri = spark._jvm.java.net.URI(s3_path)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        path_obj = spark._jvm.org.apache.hadoop.fs.Path(s3_path)

        if fs.exists(path_obj):
            print(f"[THÀNH CÔNG] Đã kết nối tới MinIO. Đường dẫn tồn tại: {s3_path}")
            file_status = fs.getFileStatus(path_obj)
            print(f"  Thông tin file: Tên: {file_status.getPath().getName()}, Kích thước: {file_status.getLen()} bytes, Là thư mục: {file_status.isDirectory()}")

            if file_status.isDirectory():
                print(f"[LỖI] Đường dẫn {s3_path} là một thư mục, không phải là file. Vui lòng cung cấp đường dẫn đến file JSONL.")
                spark.stop()
                return
            if file_status.getLen() == 0:
                print(f"[CẢNH BÁO] File {s3_path} rỗng.")
        else:
            print(f"[LỖI] Đường dẫn không tồn tại trên MinIO: {s3_path}")
            spark.stop()
            return
    except Exception as e:
        print(f"[LỖI] Không thể kết nối tới MinIO hoặc truy cập đường dẫn {s3_path}.")
        print(f"  Chi tiết lỗi: {e}")
        spark.stop()
        return

    print(f"\nĐang tiến hành đọc dữ liệu từ: {s3_path}")
    try:
        df = spark.read.format("json").load(s3_path)

        print("Đang cố gắng đọc record đầu tiên từ file...")
        
        first_record = df.first()

        if first_record:
            print("[THÀNH CÔNG] Đã đọc được record đầu tiên.")
            print("Nội dung record đầu tiên:")
            print(str(first_record))
        else:
            print("[THÔNG BÁO] Không tìm thấy record nào trong file (có thể file rỗng hoặc chỉ chứa các dòng JSON không hợp lệ).")
            
    except AnalysisException as ae:
        print(f"[LỖI] Lỗi phân tích Spark SQL: {ae}")
    except Exception as e:
        print(f"[LỖI] Đã xảy ra lỗi trong quá trình Spark xử lý dữ liệu: {e}")
    finally:
        print("\nĐang dừng SparkSession.")
        spark.stop()
        print("SparkSession đã dừng.")

if __name__ == "__main__":
    main()
