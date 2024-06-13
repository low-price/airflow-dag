from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
import psycopg2
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta
import logging

from plugins import gsheet
from plugins import s3


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()



def download_tab_in_gsheet(**context):
    url = context["params"]["url"]
    tab = context["params"]["tab"]
    table = context["params"]["table"]
    data_dir = Variable.get("DATA_DIR")

    gsheet.get_google_sheet_to_csv(
        url,
        tab,
        data_dir+'{}.csv'.format(table)
    )



def copy_to_s3(**context):
    table = context["params"]["table"]
    s3_key = context["params"]["s3_key"]

    s3_conn_id = "aws_conn_id"
    s3_bucket = "grepp-data-engineering"
    data_dir = Variable.get("DATA_DIR")
    local_files_to_upload = [ data_dir+'{}.csv'.format(table) ]
    replace = True

    s3.upload_to_s3(s3_conn_id, s3_bucket, s3_key, local_files_to_upload, replace)



# info temp 테이블 및 info 테이블 함께 조회하며 중복되지 않는 record 찾기
# 두 테이블 조인해서 id가 null인 행 추출
def check_new_ticket_info(**context):
    schema = context["params"]["schema"]
    infotable = context["params"]["infotable"]
    temptable = context["params"]["temptable"]

    logging.info("check new ticket info start")

    cursor = get_Redshift_connection()
    sql = f"""
        SELECT t.departure_date, t.departure_airport, t.arrival_date, t.arrival_airport, t.is_roundtrip
        FROM {schema}.{infotable} i
        FULL OUTER JOIN {schema}.{temptable} t
        ON 
            i.departure_date = t.departure_date
            AND i.departure_airport = t.departure_airport
            AND i.arrival_date = t.arrival_date
            AND i.arrival_airport = t.arrival_airport
            AND i.is_roundtrip = t.is_roundtrip
        WHERE i.id IS NULL;
    """
    cursor.execute(sql)
    records = cursor.fetchall()

    cursor.close()

    logging.info(f"check new ticket_info done")

    return records


def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["infotable"]
    records = context['task_instance'].xcom_pull(key="return_value", task_ids='check_new_ticket_info')

    logging.info("load started")

    cursor = get_Redshift_connection()

    cursor.execute(f"SELECT COALESCE(MAX(id), 0) + 1 FROM {schema}.{table}")
    next_id = cursor.fetchone()[0]
    logging.info(f"next_id : {next_id}")

    for record in records:
        departure_date, departure_airport, arrival_date, arrival_airport, is_roundtrip = record
        
        try:
            cursor.execute("BEGIN;")
            sql = f"""
                INSERT INTO {schema}.{table} (
                    id, departure_date, departure_airport, arrival_date, arrival_airport, is_roundtrip
                ) VALUES (
                    %s, %s, %s, %s, %s, %s
                );
            """
            cursor.execute(sql, (
                next_id,departure_date,departure_airport,arrival_date,arrival_airport,is_roundtrip
            ))
            cursor.execute("COMMIT;")

            logging.info(f"inserted {next_id},{departure_date},'{departure_airport}','{arrival_date}','{arrival_airport}','{is_roundtrip}'")
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
            logging.info("ROLLBACK")
            cursor.execute("ROLLBACK;")
            raise
        
        next_id += 1

    cursor.close()
    logging.info("load done")


dag = DAG(
    dag_id='load_flight_ticket_info',
    start_date=datetime(2024, 6, 1),  
    schedule='@once', #'*/10 * * * *',  
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 0,
        'retry_delay': timedelta(minutes=3),
    }
)

file_path = "naver_flight_ticket_info.csv"
schema = 'mool8487' #'jheon735'  
infotable = 'flight_ticket_info'
temptable = 'flight_ticket_info_temp'

# 추후 논의 후 시트 정보 수정
sheet = {
        "url": "https://docs.google.com/spreadsheets/d/1AJICer9RRJ-G3zxAc9Lk73gfK-nM0yn4gTRBK1Kbkzg/", 
        "tab": "FlightTicketInfo",
        "schema": "mool8487",
        "table": "flight_ticket_info_temp"
}

download_tab_in_gsheet = PythonOperator(
        task_id = 'download_tab_in_gsheet',
        python_callable = download_tab_in_gsheet,
        params = sheet,
        dag = dag)

s3_key = sheet["schema"] + "_" + sheet["table"]

copy_to_s3 = PythonOperator(
    task_id = 'copy_to_s3',
    python_callable = copy_to_s3,
    params = {
        "table": sheet["table"],
        "s3_key": s3_key
    },
    dag = dag)

run_copy_sql = S3ToRedshiftOperator(
    task_id = 'run_copy_sql',
    s3_bucket = "grepp-data-engineering",
    s3_key = s3_key,
    schema = sheet["schema"],
    table = sheet["table"],
    copy_options = ['csv', 'IGNOREHEADER 1'],
    method = 'REPLACE',
    redshift_conn_id = "redshift_dev_db",
    aws_conn_id = 'aws_conn_id',
    dag = dag
)

check_new_ticket_info = PythonOperator(
    task_id = 'check_new_ticket_info',
    python_callable = check_new_ticket_info,
    params = {
        "schema" : schema,
        "infotable" : infotable,
        "temptable" : temptable
    },
    dag = dag
)

load = PythonOperator(
    task_id='product_info_load',
    python_callable=load,
    params = {
        "schema" : schema,
        "infotable" : infotable
    },
    dag = dag
    )

download_tab_in_gsheet >> copy_to_s3 >> run_copy_sql >> check_new_ticket_info >> load