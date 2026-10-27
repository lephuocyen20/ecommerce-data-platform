
FROM apache/spark:3.5.6

USER root

# Python dependencies
RUN pip install --no-cache-dir minio PyYAML psycopg2-binary python-dotenv

# Hadoop S3A dependencies
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

RUN set -eux; \
    curl -fL --retry 5 \
      "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar; \
    curl -fL --retry 5 \
      "https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

USER spark


