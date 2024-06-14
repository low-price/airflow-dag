from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import psycopg2
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta
import logging
import gspread
import pandas as pd


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


import gspread
import pandas as pd
import json
from google.oauth2.service_account import Credentials
from airflow.models import Variable


def read_tab_in_gsheet_to_df(**context):
    url = context["params"]["url"]
    tab = context["params"]["tab"]

    # Google Sheets API credentials
    creds_json = Variable.get("google_sheet_access_token")
    creds_dict = json.loads(creds_json)
    
    # Set the correct scope for Google Sheets API
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)

    # Initialize the Google Sheets client
    client = gspread.authorize(creds)

    # Open the Google Sheet by URL
    sheet = client.open_by_url(url)

    # Select the specified tab
    worksheet = sheet.worksheet(tab)

    # Get all values in the worksheet
    data = worksheet.get_all_values()

    # Convert to DataFrame
    df = pd.DataFrame(data[1:], columns=data[0])

    return df


# info temp 테이블 및 info 테이블 함께 조회하며 중복되지 않는 record 찾기
# 두 테이블 조인해서 id가 null인 행 추출
def check_new_ticket_info(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    df = context["task_instance"].xcom_pull(key="return_value", task_ids='read_tab_in_gsheet_to_df')

    logging.info("check new ticket info start")

    cursor = get_Redshift_connection()

    new_tickets = []
    count = 0
    for _, row in df.iterrows():
        cursor.execute("BEGIN;")
        sql = f"""
                SELECT COUNT(*) FROM {schema}.{table} 
                WHERE departure_date=%s AND departure_airport=%s AND arrival_date=%s 
                AND arrival_airport=%s AND is_roundtrip=%s;"""
        
        cursor.execute(sql, (
            row["departure_date"], row["departure_airport"], row["arrival_date"], row["arrival_airport"], row["is_roundtrip"]
        ))
        new = cursor.fetchone()[0]
        if new == 0:
            logging.info(list(row))
            new_tickets.append(list(row))
            count += 1

    cursor.close()

    logging.info(f"check new ticket_info done")

    return new_tickets


def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    new_tickets = context['task_instance'].xcom_pull(key="return_value", task_ids='check_new_ticket_info')

    logging.info("load started")

    cursor = get_Redshift_connection()

    cursor.execute(f"SELECT COALESCE(MAX(id), 0) + 1 FROM {schema}.{table}")
    next_id = cursor.fetchone()[0]
    logging.info(f"next_id : {next_id}")

    for record in new_tickets:
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
    dag_id='load_flight_ticket_info_v2',
    start_date=datetime(2024, 6, 1),  
    schedule='@once', #'*/10 * * * *',  
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 0,
        'retry_delay': timedelta(minutes=3),
    }
)


sheet = {
        "url": "https://docs.google.com/spreadsheets/d/1AJICer9RRJ-G3zxAc9Lk73gfK-nM0yn4gTRBK1Kbkzg/", 
        "tab": "FlightTicketInfo",
        "schema": "mool8487", #'jheon735'  
        "table": "flight_ticket_info"
}

read_tab_in_gsheet_to_df = PythonOperator(
        task_id = 'read_tab_in_gsheet_to_df',
        python_callable = read_tab_in_gsheet_to_df,
        params = sheet,
        dag = dag)

check_new_ticket_info = PythonOperator(
    task_id = 'check_new_ticket_info',
    python_callable = check_new_ticket_info,
    params = sheet,
    dag = dag
)

load = PythonOperator(
    task_id='product_info_load',
    python_callable=load,
    params = sheet,
    dag = dag
    )


read_tab_in_gsheet_to_df >> check_new_ticket_info >> load