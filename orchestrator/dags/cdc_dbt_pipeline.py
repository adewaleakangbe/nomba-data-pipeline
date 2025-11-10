from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# -----------------------------
# Default arguments for the DAG
# -----------------------------
default_args = {
    'owner': 'adewale',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# -----------------------------
# DAG definition
# -----------------------------
with DAG(
    'cdc_dbt_pipeline',
    default_args=default_args,
    description='Run dbt transformations after CDC',
    schedule_interval='@daily',
    start_date=days_ago(0),
    catchup=False,
    tags=['nomba', 'cdc', 'dbt'],
) as dag:

    # Step 1: Install dbt dependencies
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command="cd /opt/airflow/dbt/nomba_analytics && dbt deps"
    )

    # Step 2: Run dbt models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            "cd /opt/airflow/dbt/nomba_analytics && "
            "dbt run --profile nomba_analytics --target dev --debug"
        )
    )

    # Step 3: Run dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=(
            "cd /opt/airflow/dbt/nomba_analytics && "
            "dbt test --profile nomba_analytics --target dev --debug"
        )
    )

    # -----------------------------
    # Task dependencies
    # -----------------------------
    dbt_deps >> dbt_run >> dbt_test

