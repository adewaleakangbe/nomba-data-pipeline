# Use official Airflow image
FROM apache/airflow:2.9.0

# Switch to root to install packages
USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    pymongo \
    psycopg2-binary \
    dbt-core==1.10.13 \
    dbt-postgres==1.9.1

# Make sure the .dbt directory exists and copy your profiles.yml
RUN mkdir -p /home/airflow/.dbt
COPY ./profiles.yml /home/airflow/.dbt/profiles.yml


# Switch back to airflow user
USER airflow

# Set workdir if needed
WORKDIR /opt/airflow

