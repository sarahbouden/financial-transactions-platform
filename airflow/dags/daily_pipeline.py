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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def check_bq_freshness(**context):
    """
    Vérifie que des données fraîches sont arrivées dans BigQuery
    dans les dernières 2 heures. Fail le DAG si rien n'est arrivé.
    """
    client = bigquery.Client(project="crucial-module-493808-q9")
    query = """
        SELECT COUNT(*) as recent_count
        FROM `crucial-module-493808-q9.fintech_dw.raw_transactions`
        WHERE publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
    """
    result = client.query(query).result()
    row = list(result)[0]
    recent_count = row.recent_count

    logger.info(f"Transactions reçues dans les 2 dernières heures : {recent_count}")

    if recent_count == 0:
        raise ValueError(
            "Aucune transaction fraîche dans BigQuery depuis 2 heures. "
            "Vérifier le producer et les connecteurs Kafka."
        )

    return recent_count


with DAG(
    dag_id="daily_pipeline",
    description="Pipeline quotidien : check fraîcheur → dbt run → dbt test",
    schedule_interval="0 6 * * *",   # Tous les jours à 6h UTC
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["fintech", "dbt", "daily"],
) as dag:

    check_freshness = PythonOperator(
        task_id="check_bq_freshness",
        python_callable=check_bq_freshness,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select staging --profiles-dir {DBT_PROFILES_DIR} --no-use-colors"
        ),
    )

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select intermediate --profiles-dir {DBT_PROFILES_DIR} --no-use-colors"
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select marts --profiles-dir {DBT_PROFILES_DIR} --no-use-colors"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --no-use-colors"
        ),
    )

    # Ordre d'exécution séquentiel
    check_freshness >> dbt_run_staging >> dbt_run_intermediate >> dbt_run_marts >> dbt_test