from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.python import ShortCircuitOperator, PythonOperator
import psycopg2
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
from datetime import timedelta
import requests
import logging


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def read_urls(url_path):
    logging.info('url file read start')
    with open(url_path, 'r') as f:
        urls = f.read().splitlines()
    urls = [x for x in urls if x != '']
    logging.info(urls)
    logging.info('url fine read done')
    return urls

def check_new_urls(schema, table, urls):
    logging.info("check new urls start")
    urls = urls.replace("'", '"')
    urls = json.loads(urls)
    new_urls = []
    count = 0
    cur = get_Redshift_connection() 
    for url in urls:
        cur.execute("BEGIN;")
        sql = f"SELECT COUNT(*) FROM {schema}.{table} WHERE url='{url}';"
        cur.execute(sql)
        new = cur.fetchone()[0]
        if new == 0:
            new_urls.append(url)
            count += 1
    logging.info(f"check new urls done, number of new url : {count}")
    return new_urls

def short_circuit_condition(urls):
    urls = urls.replace("'", '"')
    urls = json.loads(urls)
    return len(urls) > 0

def get_product_info(urls):
    urls = urls.replace("'", '"')
    urls = json.loads(urls)
    infos = []
    for url in urls:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.coupang.com/"
        }
        response = requests.get(url, headers=headers, verify=False)
        # logging.info(response.content.decode('utf-8'))m
        fullurl = response.url.split("=")
        itemID = fullurl[1].split("&")[0]
        venderID = fullurl[2].split("&")[0]
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            name = soup.select_one(".prod-buy-header__title").get_text(strip=True)
            img_url = soup.select_one(".prod-image__detail")["src"][2:]
        else:
            logging.error(f"Failed to retrieve the webpage. Status code: {response.status_code}")
        infos.append([name, itemID , venderID, url, img_url])
    return infos

def product_info_load(schema, table, iteminfos):
    logging.info("product info load started")
    iteminfos = iteminfos.replace("'", '"')
    iteminfos = json.loads(iteminfos)
    for iteminfo in iteminfos:
        name, itemID , venderID, url, img_url = iteminfo
        cur = get_Redshift_connection()
        try:
            cur.execute("BEGIN;")
            cur.execute(f"INSERT INTO {schema}.{table} (item_id, vendor_item_id, url, product_name, image_url) VALUES ({itemID},{venderID},'{url}','{name}','{img_url}');")
            cur.execute("COMMIT;")
            logging.info(f"inserted {name} info")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            logging.info("ROLLBACK")
            cur.execute("ROLLBACK;")


with DAG(
    dag_id='load_product_info',
    start_date=datetime(2024, 6, 1),  
    schedule='@once', #'*/10 * * * *',  
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 0,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    url_path = "./coupang_final.txt"
    schema = 'jheon735'  
    infotable = 'coupang_product_info'

    readurls = PythonOperator(
        task_id = 'read_urls',
        python_callable = read_urls,
        op_args = [url_path],
        dag = dag)

    checknewurls = PythonOperator(
        task_id = 'check_new_urls',
        python_callable = check_new_urls,
        op_args = [schema, infotable, "{{ ti.xcom_pull(task_ids='read_urls') }}"],
        dag = dag)

    short_circuit = ShortCircuitOperator(
        task_id='short_circuit',
        python_callable=short_circuit_condition,
        op_args = ["{{ ti.xcom_pull(task_ids='check_new_urls') }}"],
        dag = dag  
    )

    getproductinfo = PythonOperator(
        task_id='get_product_info',
        python_callable=get_product_info,
        op_args = ["{{ ti.xcom_pull(task_ids='check_new_urls') }}"],
        dag = dag
    )

    productinfoload = PythonOperator(
        task_id='product_info_load',
        python_callable=product_info_load,
        op_args = [schema, infotable, "{{ ti.xcom_pull(task_ids='get_product_info') }}"],
        dag = dag
    )

    readurls >> checknewurls >> short_circuit >> getproductinfo >> productinfoload
