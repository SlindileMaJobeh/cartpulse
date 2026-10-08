# One image for the simulator, the Spark streaming app, the API and bootstrap.
FROM python:3.11-slim-bookworm

# Spark needs a JVM (works on amd64 and Apple Silicon).
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/cartpulse
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY cartpulse ./cartpulse

ENV PYTHONPATH=/opt/cartpulse \
    PYTHONUNBUFFERED=1 \
    SPARK_IVY_DIR=/data/.ivy
