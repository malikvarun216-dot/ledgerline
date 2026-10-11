# ledgerline — the Airflow image (Session 16; decisions.md "Airflow runs one DAG per dims night").
# Airflow itself, unchanged, plus a separate venv holding the pipeline's own dependencies. Tasks
# call /opt/pipeline/bin/python scripts/..., the same scripts a person runs from the laptop.
FROM apache/airflow:3.1.0-python3.11

USER root
RUN mkdir /opt/pipeline && chown airflow: /opt/pipeline
USER airflow

COPY requirements-pipeline.txt /tmp/requirements-pipeline.txt
RUN python -m venv /opt/pipeline \
 && /opt/pipeline/bin/pip install --no-cache-dir -r /tmp/requirements-pipeline.txt
