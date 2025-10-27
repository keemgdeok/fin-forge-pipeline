FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_LOCAL_IP=127.0.0.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Install project dependencies alongside PySpark so integration tests can run.
COPY requirements.txt /tmp/pip/requirements.txt
RUN pip install --no-cache-dir pyspark>=3.5.1 \
    && pip install --no-cache-dir -r /tmp/pip/requirements.txt

# Default command runs a shell; scripts bind-mount the repo for pytest execution.
CMD ["bash"]
