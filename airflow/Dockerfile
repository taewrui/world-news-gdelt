FROM apache/airflow:2.11.0

# Switch to root to install system packages
USER root

# Install system dependencies if needed
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Copy custom modules (if any)
# COPY ./dags /opt/airflow/dags
# COPY ./plugins /opt/airflow/plugins
