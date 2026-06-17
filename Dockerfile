# Extend the official Airflow image with this project's pipeline code and deps.
# Constraint: all compose/Docker assets live in-repo — no curl-to-shell bootstrap.
FROM apache/airflow:3.2.2-python3.12

# pip must run as the airflow user (not root) — see Airflow docker-stack docs.
COPY requirements.txt requirements-airflow.txt /tmp/
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-airflow.txt

COPY --chown=airflow:root src /opt/airflow/src
COPY --chown=airflow:root config /opt/airflow/config
COPY --chown=airflow:root dags /opt/airflow/dags

ENV PYTHONPATH=/opt/airflow