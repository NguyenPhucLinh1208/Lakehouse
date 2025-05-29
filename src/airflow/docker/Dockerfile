ARG AIRFLOW_BASE_IMAGE_VERSION=3.0.1
FROM apache/airflow:${AIRFLOW_BASE_IMAGE_VERSION}

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        procps \
        openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN mkdir -p /opt/project_code && chown airflow:root /opt/project_code && \
    mkdir -p /tmp/airflow_requirements && chown airflow:root /tmp/airflow_requirements

COPY --chown=airflow:root src/airflow/docker/requirements_airflow.txt /tmp/airflow_requirements/requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /tmp/airflow_requirements/requirements.txt && rm -rf /tmp/airflow_requirements

WORKDIR ${AIRFLOW_HOME:-/opt/airflow}

