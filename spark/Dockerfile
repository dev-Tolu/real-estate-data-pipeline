FROM public.ecr.aws/bitnami/spark:3.5

USER root

RUN pip install --no-cache-dir \
    boto3 \
    pandas \
    numpy \
    psycopg2-binary \
    beautifulsoup4

COPY etl.py /opt/spark-apps/etl.py

WORKDIR /opt/spark-apps

USER 1001