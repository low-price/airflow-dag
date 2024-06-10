from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import psycopg2
import requests
from bs4 import BeautifulSoup
import re

from datetime import datetime
from datetime import timedelta

import requests
import logging


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def get_coupang_price(url):
    logging.info(datetime.utcnow())
    logging.info(url)
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.coupang.com/"
    }
    response = requests.get(url, headers=headers, verify=False)
    # logging.info(response.content.decode('utf-8'))
    urls = response.url.split("=")
    itemID = urls[1].split("&")[0]
    venderID = urls[2].split("&")[0]
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        name = soup.select_one(".prod-buy-header__title").get_text(strip=True)[:5]
        price_element = soup.find('div', class_='prod-coupon-price price-align major-price-coupon').select_one('.total-price strong')
        if price_element:
            price = price_element.get_text(strip=True)
            numbers = re.findall(r'\d+',price)
            price = int(''.join(numbers))
            logging.info(f"{itemID} - {venderID} 쿠팡 현재 가격 : {price}")
        else:
            price_element = soup.select_one('.total-price strong')
            if price_element:
                price = price_element.get_text(strip=True)
                numbers = re.findall(r'\d+',price)
                price = int(''.join(numbers))
                logging.info(f"{itemID} - {venderID} 쿠팡 현재 가격 : {price}")
            else:
                logging.info("URL을 다시 입력해주세요")
    else:
        logging.error(f"Failed to retrieve the webpage. Status code: {response.status_code}")
    return [name, itemID , price]


@task
def create_table(schema, iteminfo):
    logging.info("create table started")
    name, itemID, price = iteminfo    
    cur = get_Redshift_connection()   
    try:
        cur.execute("BEGIN;")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{name}{itemID} (datetime timestamp,price int);")
        cur.execute("COMMIT;") 
        logging.info(f"created {name}{itemID} table done")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")   


@task
def load(schema, iteminfo):
    logging.info("load started")
    name, itemID, price = iteminfo    
    cur = get_Redshift_connection()   

    try:
        cur.execute("BEGIN;")
        sql = f"INSERT INTO {schema}.{name}{itemID} VALUES ('{datetime.utcnow()}', '{price}')"
        cur.execute(sql)
        cur.execute("COMMIT;") 
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")   
    logging.info("load done")


with DAG(
    dag_id='get_price_coupang',
    start_date=datetime(2024, 6, 1),  # 날짜가 미래인 경우 실행이 안됨
    schedule='@once', #'*/10 * * * *',   적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 0,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:

    url = "https://link.coupang.com/a/bEPwcW"
    schema = 'jheon735'   ## 자신의 스키마로 변경

    iteminfo = get_coupang_price(url)
    # itemID, venderID, price = iteminfo
    create_table(schema, iteminfo)
    load(schema, iteminfo)
