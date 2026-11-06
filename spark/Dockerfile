
FROM apache/spark:3.5.6

USER root

# Iceberg 1.12 is compiled for Java 17. The Spark 3.5 base image ships with
# Java 11, so install and select Java 17 before adding the Iceberg runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && update-alternatives --set java /usr/lib/jvm/java-17-openjdk-amd64/bin/java

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Python dependencies
RUN pip install --no-cache-dir minio PyYAML psycopg2-binary python-dotenv pytest

# Hadoop S3A dependencies
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG ICEBERG_VERSION=1.12.0

RUN set -eux; \
    curl -fL --retry 5 \
      "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar; \
    curl -fL --retry 5 \
      "https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Iceberg's Spark 3.5 runtime includes the table and catalog integration.
# The AWS bundle provides Iceberg's S3FileIO used by the Nessie REST catalog.
RUN set -eux; \
    curl -fL --retry 5 \
      "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar; \
    curl -fL --retry 5 \
      "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      -o /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

COPY spark/conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark


