FROM apache/airflow:3.2.1-python3.12

# Switch to root to install system-level deps (e.g. for psycopg2)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    openjdk-17-jre-headless \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

# JAVA_HOME needed by PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Switch back to the airflow user before pip install
USER airflow

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir \
    -r /tmp/requirements-airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.12.txt"
