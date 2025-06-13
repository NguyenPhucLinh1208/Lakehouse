from pyspark.sql import SparkSession    
from pyspark.sql.functions import (

)
from pyspark.sql.types import (

)
from pyspark.sql.window import Window
import argparse
from dotenv import load_dotenv
import os
from prometheus_client import CollectorRegistry, Gause, push_to_gateway
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

app_name = "...."

# timezone
VIETNAM_TZ = pytz.timezone('Asia/Ho_Chi_Minh')
PYTHON_DATE_FORMAT_PATTERN = "%Y-%m-%d"
SPARK_SQL_DATE_FORMAT_PATTERN = "yyyy-MM-dd"

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument(
    "--name",
    type=str,
    default=None,
    help="aaaaa"
)
args = arg_parser.parse_args()


registry = CollectorRegistry()

g_metric_name = Gause( name, description, label, registry=registry)

def push_metrics_to_gate_way():



