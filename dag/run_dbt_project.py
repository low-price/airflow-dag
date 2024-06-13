from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2022, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'run_dbt_project',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    run_dbt_project = SSHOperator(
        task_id='run_dbt_project',
        ssh_conn_id='test_ssh_conn',
        command='cd ~/lowprice && dbt test && dbt run',
        dag=dag
    )

    run_dbt_project
