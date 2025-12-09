# Python 3.11
FROM python:3.11-slim-bookworm

# 1. Java 17 for Spark 4.x
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  openjdk-17-jre-headless \
  procps \
  curl \
  && rm -rf /var/lib/apt/lists/* \
  && ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH="$JAVA_HOME/bin:${PATH}"

# 2. Core Python deps
# - PySpark 4.0.1
# - Delta Lake (delta-spark) 4.0.0
# - pandas 2.3.3 (modern)
# - Dask 2025.11.0 (with dataframe extra)
# - Feast 0.57.0 with DuckDB offline store
RUN pip install --no-cache-dir \
  "pyspark==4.0.1" \
  "delta-spark==4.0.0" \
  "pandas==2.3.3" \
  "dask[dataframe]==2025.11.0" \
  "feast[duckdb]==0.57.0" \
  s3fs \
  pyarrow \
  openpyxl \
  redis \
  boto3

# 3. JARs for Hadoop AWS (S3) + AWS SDK + spark-excel (Spark 4)
WORKDIR /usr/local/lib/python3.11/site-packages/pyspark/jars

# Hadoop AWS & AWS Java SDK bundle
RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar && \
  curl -fSL -O https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar && \
  curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.4.1/hadoop-common-3.4.1.jar

# spark-excel for Spark 4.0.x / Scala 2.13
RUN curl -O https://repo1.maven.org/maven2/dev/mauch/spark-excel_2.13/4.0.0_0.31.2/spark-excel_2.13-4.0.0_0.31.2.jar

# 4. Back to app working dir
WORKDIR /app

CMD ["python3"]
