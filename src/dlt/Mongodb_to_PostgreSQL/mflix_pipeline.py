# mflix_pipeline.py
import dlt
import os

# Import trực tiếp source function chính từ package source đã tạo
from mflix_mongodb_source import mflix_main_source

def run_mflix_pipeline(
    pipeline_name: str = "mflix_mongo_pipeline",
    dataset_name: str = "mflix_staging",
    destination_type: str = "postgres"
):
    """
    Defines and runs the DLT pipeline for MFlix data.
    Reads configuration from DLT files and environment variables.
    """
    print(f"--- Starting Pipeline: {pipeline_name} ---")
    print(f"Destination: {destination_type}, Dataset: {dataset_name}")

    # 1. Initialize the DLT pipeline
    # DLT will automatically find credentials for 'postgres' destination
    # from secrets.toml ([destination.postgres].credentials) or env vars.
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination_type,
        dataset_name=dataset_name,
    )

    # 2. Get data from the source
    # The mflix_main_source function itself reads its necessary config (db, collections etc.)
    # from config.toml / env vars via dlt.config.value / dlt.secrets.value
    print("Requesting data from source 'mflix_mongodb'...")
    source_data = mflix_main_source() # Không cần truyền config vào đây nữa

    # 3. Run the pipeline
    if not source_data:
         print("Source function did not yield any resources to run. Exiting.")
         return

    print(f"Running pipeline '{pipeline.pipeline_name}' to load data...")
    load_info = pipeline.run(source_data)

# 4. Inspect the load outcome
    print("--- Pipeline Run Finished ---")
    print(load_info)

    # Sửa lại cách kiểm tra lỗi:
    failed_packages_exist = any(pkg.state == 'failed' for pkg in load_info.load_packages)
    if failed_packages_exist:
        print("\n[ERROR] Some data packages failed to load. Check logs and destination.")
        # Consider raising an error for orchestration tools
        # raise RuntimeError(f"Pipeline {pipeline_name} failed.")
    else:
        print("\n[SUCCESS] All data packages loaded successfully.")

    print(f"Pipeline state and runtime data stored in: {pipeline._pipeline_storage.storage_path}") # Thêm dấu gạch dưới

    print(f"------------------------------")
    
if __name__ == "__main__":
   run_mflix_pipeline()

