FROM astrocrpublic.azurecr.io/runtime:3.1-11

RUN mkdir -p /usr/local/airflow/include/transforms/logs \
    && chown -R astro:astro /usr/local/airflow/include/transforms/logs