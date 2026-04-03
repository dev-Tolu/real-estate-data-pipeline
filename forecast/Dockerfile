FROM python:3.11-slim
 
WORKDIR /app
 
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*
 
RUN pip install --no-cache-dir \
    pandas \
    numpy \
    prophet \
    psycopg2-binary \
    sqlalchemy \
    redis \
    joblib \
    scikit-learn \
    plotly
 
COPY forecast.py /app/forecast.py
 
CMD ["python", "forecast.py"]
 