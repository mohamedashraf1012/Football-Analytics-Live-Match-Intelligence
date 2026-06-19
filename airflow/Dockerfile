FROM apache/airflow:2.9.2

USER root

# Java 11 needed for PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-11-jdk \
        procps \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Separate Spark temp dir to avoid filling /tmp
RUN mkdir -p /opt/spark-temp && chmod 777 /opt/spark-temp

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_LOCAL_DIR=/opt/spark-temp
