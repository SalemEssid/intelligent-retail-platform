FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && apt-get install -y gcc python3-dev

USER airflow
RUN pip install --no-cache-dir \
    'numpy<2' \
    pandas \
    scikit-learn \
    psycopg2-binary \
    pyarrow \
    xgboost
