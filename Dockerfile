
FROM apache/airflow:3.2.1
USER root
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    && apt-get clean
USER airflow
RUN pip install apache-airflow-providers-postgres \
    apache-airflow-providers-common-sql \
    beautifulsoup4 \
    requests \
    psycopg2-binary \
    selenium