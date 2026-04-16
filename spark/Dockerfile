FROM public.ecr.aws/bitnami/spark:3.5

USER root

# Install extra dependencies into the Bitnami Python interpreter.
RUN /opt/bitnami/python/bin/pip install --no-cache-dir \
    boto3 \
    psycopg2-binary \
    beautifulsoup4 \
    lxml


# Download JARs directly to the Spark classpath
RUN curl -Lo /opt/bitnami/spark/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && \
    curl -Lo /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -Lo /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    chmod 644 /opt/bitnami/spark/jars/postgresql-42.6.0.jar \
              /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
              /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Ensure UID 1001 has a passwd entry.
RUN grep -q "^spark:" /etc/passwd \
    || echo "spark:x:1001:1001:Spark User:/opt/bitnami/spark:/bin/bash" >> /etc/passwd

# Verify the install
RUN /opt/bitnami/python/bin/python3 -c "\
import pyspark; \
import bs4; \
import boto3; \
print('All imports OK — pyspark', pyspark.__version__, '/ bs4', bs4.__version__)"

COPY etl.py /opt/spark-apps/etl.py

WORKDIR /opt/spark-apps

USER 1001