# apps/test_pyspark.py
from pyspark.sql import SparkSession
import time
import traceback # Import traceback để in lỗi chi tiết hơn

print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Script started...")
spark = None # Khởi tạo biến spark bên ngoài try

try:
    # Khởi tạo SparkSession
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Attempting to create SparkSession...")
    spark = SparkSession.builder \
        .appName("PySparkLongRunTest") \
        .getOrCreate()

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] SparkSession created successfully!")
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Spark version: {spark.version}")
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Spark UI likely available at: http://<driver-ip>:4040 (check logs for exact address and port)")


    # Tạo một DataFrame nhỏ để thử nghiệm
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Creating a sample DataFrame...")
    data = [("Test User 1", 101), ("Test User 2", 102)]
    columns = ["User", "UserID"]
    df = spark.createDataFrame(data, columns)

    # Thực hiện một action đơn giản (show) để kích hoạt tính toán
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Performing df.show()...")
    df.show()

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Basic Spark operations successful!")

    # --- THÊM ĐOẠN DỪNG TẠI ĐÂY ---
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] =======================================================")
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Application is now paused.")
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] You can access the Spark UI now.")
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Common URL: http://localhost:4040 (or check logs for the 'SparkUI' service port)")
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] =======================================================")
    input(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Press Enter in this terminal to stop the SparkSession and finish the script...")
    # -----------------------------

except Exception as e:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] An error occurred: {e}")
    traceback.print_exc() # In chi tiết lỗi

finally:
    # Đảm bảo dừng SparkSession ngay cả khi có lỗi hoặc sau khi nhấn Enter
    if spark: # Kiểm tra xem spark đã được khởi tạo thành công chưa
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Stopping SparkSession...")
        spark.stop()
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] SparkSession stopped.")

print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Script finished.")
