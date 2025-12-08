# Use Python 3.9 Slim (Standard for ML)
FROM python:3.9-slim-bookworm

# 1. Install Java 17 (Required for Spark 3.5+)
# Install procps to enable 'ps' command, often needed by Spark monitoring
RUN apt-get update && \
  apt-get install -y openjdk-17-jre-headless procps curl && \
  rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk

# 2. Set JAVA_HOME Environment Variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

# 3. Install Python Libraries
# pyspark==3.5.0: Matches the Hadoop/Delta versions in our script
# delta-spark==3.0.0: The Python client for Delta Lake
# pandas: For data manipulation if needed
RUN pip install --no-cache-dir \
  pyspark==3.5.0 \
  delta-spark==3.0.0 \
  pandas \
  openpyxl

# 4. Set Working Directory
WORKDIR /usr/local/lib/python3.9/site-packages/pyspark/jars

# A. Delta Lake
RUN curl -O https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar
RUN curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar

# B. Hadoop AWS & AWS SDK (Matching Spark 3.5 / Hadoop 3.3.4)
RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
RUN curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 5. Default Command (Can be overridden by Airflow)
CMD ["python3"]
