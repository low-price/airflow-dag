from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import psycopg2

from selenium import webdriver
from selenium.webdriver.chrome.service import Service   #드라이버는 브라우저마다 다르다
from webdriver_manager.chrome import ChromeDriverManager    # pc에 설치된 크롬과 버전을 같게 하기 위해
from selenium.webdriver.common.by import By     # 응답 요소에서 특정 요소를 추출하는 메서드
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

import re

from datetime import datetime
from datetime import timedelta

import requests
import logging

import time


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def get_naver_flight_price(departure_date, arrival_date, departure_airport, arrival_airport, roundtrip = True):
    logging.info(datetime.utcnow())
    logging.info(f"출발일: {departure_date}, 출발지: {departure_airport}, 도착일: {arrival_date}, 도착지: {arrival_airport}, 왕복 여부: {roundtrip}")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

    if roundtrip:
        url = f"https://flight.naver.com/flights/international/{departure_airport}-{arrival_airport}-{departure_date}/"\
                    f"{arrival_airport}-{departure_airport}-{arrival_date}?adult=1&fareType=Y"
    else:    
        url = f"https://flight.naver.com/flights/international/{departure_airport}-{arrival_airport}-{departure_date}?adult=1&fareType=Y"
    logging.info(url)

    driver.get(url)

    try:
        # 모든 정보가 로딩 되기까지 시간이 걸림. 최소값을 못 가져오는 상황을 방지
        time.sleep(10)

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        # 최저가 정보 div 찾기
        if roundtrip: #왕복
            inner_div = soup.select_one('.concurrent_inner__OXzRp')
        else:
            inner_div = soup.select_one('.indivisual_inner__6ST3H')
        # print(inner_div.prettify)

        # 가격
        price = inner_div.select_one('.item_num__aKbk4').get_text(strip=True)
        # print(f"price: {price}")
        logging.info(f"항공권 최저가 : {price}")

        if roundtrip: # 왕복
            # 항공사
            airline = inner_div.find_all(class_='airline_name__0Tw5w')

            departure_airline = airline[0].get_text(strip=True).split(",")[0]
            if len(airline) == 1: # 출발, 도착 모두 같은 항공사일 경우
                arrival_airline = departure_airline
            else:
                arrival_airline = airline[1].get_text(strip=True).split(",")[0]
            logging.info(f"출발시 항공사 : {departure_airline}, 도착시 항공사: {arrival_airline}")

            # 출발시 경유 횟수, 소요 시간
            layover_count = inner_div.find_all(class_='route_details__F_ShG')
            departure_layover_count, departure_duration = layover_count[0].get_text(strip=True).split(",")
            arrival_layover_count, arrival_duration = layover_count[1].get_text(strip=True).split(",")
            
            departure_layover_count = int(departure_layover_count[3:])
            departure_duration = departure_duration.strip()

            arrival_layover_count = int(arrival_layover_count[3:])
            arrival_duration = arrival_duration.strip()

            logging.info(f"출발시 경유 횟수 : {departure_layover_count}, 출발시 소요 시간: {departure_duration}")
            logging.info(f"도착시 경유 횟수 : {arrival_layover_count}, 도착시 소요 시간: {arrival_duration}")

            # 출발시 이륙 시간 및 착륙 시간 / 도착시 이륙 시간 및 착륙 시간
            route_times = inner_div.find_all(class_="route_time__xWu7a")

            departure_takeoff_time = route_times[0].get_text(strip=True)
            departure_landing_time = route_times[1].get_text(strip=True)

            arrival_takeoff_time = route_times[2].get_text(strip=True)
            arrival_landing_time = route_times[3].get_text(strip=True)

            logging.info(f"출발시 이륙 시간 : {departure_takeoff_time}, 출발시 착륙 시간: {departure_landing_time}")
            logging.info(f"도착시 이륙 시간 : {arrival_takeoff_time}, 도착시 착륙 시간: {arrival_landing_time}")
            
        else:
            # 항공사
            departure_airline = inner_div.select_one('.airline_name__0Tw5w').get_text(strip=True)

            logging.info(f"항공사 : {departure_airline}")

            # 경유 횟수, 소요 시간
            departure_layover_count, departure_duration = inner_div.select_one('.route_details__F_ShG').get_text(strip=True).split(",")

            departure_layover_count = int(departure_layover_count[3:])
            departure_duration = departure_duration.strip()

            logging.info(f"출발시 경유 횟수 : {departure_layover_count}, 출발시 소요 시간: {departure_duration}")

            # 이륙 시간, 착륙 시간
            route_times = inner_div.find_all(class_="route_time__xWu7a"),

            departure_takeoff_time = route_times[0].get_text(strip=True)
            departure_landing_time = route_times[1].get_text(strip=True)

            logging.info(f"출발시 이륙 시간 : {departure_takeoff_time}, 출발시 착륙 시간: {departure_landing_time}")
    except:
        pass
        # raise
    finally:
        driver.quit()
    
    # return [name, itemID , price]


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


def get_flight_info():
    cursor = get_Redshift_connection()  

    cursor.execute('SELECT * FROM table_name')  # 테이블 이름 설정해야함
    records = cursor.fetchall()

    cursor.close()

    return records


with DAG(
    dag_id='get_naver_flight_price',
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
    schema = 'mool8487'   ## 자신의 스키마로 변경

    records = get_flight_info()

    for record in records:
        # departure_date, departure_airflow, arrival_date, arrival_airport
        data = get_naver_flight_price(record)

        create_table(schema, data)
        load(schema, data)