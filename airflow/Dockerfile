FROM apache/airflow:3.1.8

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    cloudscraper \
    "dbt-postgres>=1.7.0" \
    "great-expectations>=0.18.0" \
    pandas \
    numpy \
    psycopg2-binary \
    sqlalchemy \
    minio==7.2.0 \
    redis \
    beautifulsoup4 \
    "tenacity>=8.3.0" \
    "protobuf>=3.19,<6.33.6" \
    statsforecast==1.7.5 \
    statsmodels \
    scikit-learn \
    joblib==1.5.3 \
    plotly==5.15.0