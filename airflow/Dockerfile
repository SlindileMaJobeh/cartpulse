FROM apache/airflow:2.10.5-python3.11

# Java for the Spark sessionize job.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*
USER airflow

# Pin Airflow so these extras can't upgrade it underneath us.
RUN pip install --no-cache-dir "apache-airflow==2.10.5" pyspark==3.5.3 pandas==2.2.3 pyarrow==16.1.0 \
      "psycopg[binary]==3.2.3" requests==2.32.3

# dbt in its own virtualenv: its pins clash with Airflow's.
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.10.13 dbt-postgres==1.9.1
ENV PATH="${PATH}:/home/airflow/dbt-venv/bin"
