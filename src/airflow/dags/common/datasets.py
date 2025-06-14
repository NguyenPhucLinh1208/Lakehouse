from airflow.datasets import Dataset

raw_vnexpress_data_ready = Dataset("signal://raw/vnexpress/daily_ready")
clean_vnexpress_data_ready = Dataset("signal://clean/vnexpress/daily_ready")
