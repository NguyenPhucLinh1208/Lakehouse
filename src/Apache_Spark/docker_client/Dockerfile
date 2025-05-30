FROM python:3.10.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
        bash \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY ./src/Apache_Spark/docker_client/requirement_spark_client.txt /app/requirement_spark_client.txt
WORKDIR /app
RUN pip install --no-cache-dir -r requirement_spark_client.txt

RUN mkdir -p /app/scripts && \
    mkdir -p /app/jars

WORKDIR /app/scripts
