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
def get_urls(schema, table):
    logging.info("get urls started")
    cur = get_Redshift_connection()   
    cur.execute("BEGIN;")
    sql = f"SELECT url FROM {schema}.{table};"
    cur.execute(sql)
    url_records = cur.fetchall()
    url_list = [record[0] for record in url_records]
    return url_list


@task
def get_price(urls):
    prices = []
    for url in urls:
        logging.info(datetime.utcnow())
        logging.info(url)
        headers = {
            "authority": "weblog.coupang.com",
            "scheme": "https",
            "origin": "https://www.coupang.com",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "macOS",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Whale/3.20.182.14 Safari/537.36",
            "cookie": "PCID=31489593180081104183684; _fbp=fb.1.1644931520418.1544640325; gd1=Y; X-CP-PT-locale=ko_KR; MARKETID=31489593180081104183684; sid=03ae1c0ed61946c19e760cf1a3d9317d808aca8b; overrideAbTestGroup=%5B%5D; x-coupang-origin-region=KOREA; x-coupang-accept-language=ko_KR;"
        }
        response = requests.get(url, headers=headers, verify=False)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            price_element = soup.find('div', class_='prod-coupon-price price-align major-price-coupon').select_one('.total-price strong')
            if price_element:
                price = price_element.get_text(strip=True)
                numbers = re.findall(r'\d+',price)
                price = int(''.join(numbers))
                logging.info(f"{url} : {price}")
            else:
                price_element = soup.select_one('.total-price strong')
                if price_element:
                    price = price_element.get_text(strip=True)
                    numbers = re.findall(r'\d+',price)
                    price = int(''.join(numbers))
                    logging.info(f"{url} : {price}")
                else:
                    logging.info("URL을 다시 입력해주세요")
            prices.append([url, price])
        else:
            logging.error(f"Failed to retrieve the webpage. Status code: {response.status_code}")
    return prices


@task
def product_price_load(schema, infotable, loadtable, prices):
    logging.info("product price load started")
    cur = get_Redshift_connection()
    for item in prices:
        url, price  = item
        try:
            cur.execute("BEGIN;")
            sql = f"SELECT id FROM {schema}.{infotable} WHERE url='{url}';"
            cur.execute(sql)
            id = cur.fetchone()[0]
            logging.info(id)
            sql = f"INSERT INTO {schema}.{loadtable} VALUES ('{datetime.utcnow()}', {id}, {price})"
            cur.execute(sql)
            cur.execute("COMMIT;")
            logging.info("load done")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            logging.info("ROLLBACK")
            cur.execute("ROLLBACK;")


with DAG(
    dag_id='get_price_coupang_v2',
    start_date=datetime(2024, 6, 1),
    schedule='0 */1 * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 0,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    schema = 'jheon735'   
    infotable = 'coupang_product_info'
    pricetalbe = 'coupang_product_price_history'

    urls = get_urls(schema, infotable)
    prices = get_price(urls)
    product_price_load(schema, infotable, pricetalbe, prices)
