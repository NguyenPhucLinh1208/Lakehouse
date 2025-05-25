#!/bin/bash

# Đây là comment giải thích mục đích của script
# Chạy Spark job để đọc dữ liệu JSONL từ MinIO

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --jars /opt/spark/apps/jars/hadoop-aws-3.3.4.jar,/opt/spark/apps/jars/aws-java-sdk-bundle-1.12.783.jar,/opt/spark/apps/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar,/opt/spark/apps/jars/nessie-spark-extensions-3.5_2.12-0.103.5.jar \
  /opt/spark/apps/curated_data_model.py
