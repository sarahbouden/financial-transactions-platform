from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import logging

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/opt/airflow/dbt/fintech_dw"
DBT_PROFILES_DIR = "/opt/airflow/dbt/fintech_dw"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


def check_row_counts(**context):
    """
    Vérifie que les tables marts ont des volumes cohérents.
    Fail si fct_transactions est vide.
    """
    client = bigquery.Client(project="crucial-module-493808-q9")

    checks = {
        "fct_transactions": "crucial-module-493808-q9.fintech_dw_marts.fct_transactions",
        "dim_merchants": "crucial-module-493808-q9.fintech_dw_marts.dim_merchants",
        "dim_transaction_types": "crucial-module-493808-q9.fintech_dw_marts.dim_transaction_types",
    }

    for table_name, full_table_id in checks.items():
        query = f"SELECT COUNT(*) as cnt FROM `{full_table_id}`"
        result = client.query(query).result()
        count = list(result)[0].cnt
        logger.info(f"{table_name}: {count} rows")

        if table_name == "fct_transactions" and count == 0:
            raise ValueError(f"Table {table_name} est vide — pipeline en échec")

    return "Row counts OK"


with DAG(
    dag_id="weekly_maintenance",
    description="Maintenance hebdomadaire : dbt test complet + row counts + docs",
    schedule_interval="0 7 * * 1",   # Tous les lundis à 7h UTC
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["fintech", "dbt", "weekly"],
) as dag:

    dbt_test_full = BashOperator(
        task_id="dbt_test_full",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --no-use-colors"
        ),
    )

    check_counts = PythonOperator(
        task_id="check_row_counts",
        python_callable=check_row_counts,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt docs generate --profiles-dir {DBT_PROFILES_DIR} --no-use-colors"
        ),
    )

    dbt_test_full >> check_counts >> dbt_docs