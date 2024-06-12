from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2022, 1, 1),
}

with DAG(
    'run_dbt_project',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    run_dbt_project = BashOperator(
        task_id='run_dbt_project',
        bash_command='cd /path/to/dbt/project && dbt run',
    )