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
def get_naver_flight_price(departure_date, departure_airport, arrival_date, arrival_airport, roundtrip = True):
    print(departure_date, type(departure_date), arrival_date, type(arrival_date))
    departure_date = departure_date.strftime('%Y%m%d')
    arrival_date = arrival_date.strftime('%Y%m%d')

    logging.info(datetime.utcnow())
    logging.info(f"출발일: {departure_date}, 출발지: {departure_airport}, 도착일: {arrival_date}, 도착지: {arrival_airport}, 왕복 여부: {roundtrip}")

    options = webdriver.ChromeOptions()
    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.4.1.6 Safari/537.36"
    options.add_argument('user-agent=' + user_agent)
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

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

            # 편도일 경우 Null 값이 되는 컬럼
            arrival_layover_count, arrival_airline, arrival_duration, arrival_takeoff_time, arrival_landing_time = None, None, None, None, None

            return [price, departure_airline, departure_layover_count, departure_duration, departure_takeoff_time, departure_landing_time,\
                arrival_layover_count, arrival_airline, arrival_duration, arrival_takeoff_time, arrival_landing_time]

    except:
        pass
        # raise
    finally:
        driver.quit()
    
    


# @task
# def create_table(schema, iteminfo):
#     logging.info("create table started")
#     name, itemID, price = iteminfo    
#     cur = get_Redshift_connection()   
#     try:
#         cur.execute("BEGIN;")
#         cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{name}{itemID} (datetime timestamp, price int);")
#         cur.execute("COMMIT;") 
#         logging.info(f"created {name}{itemID} table done")
#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)
#         cur.execute("ROLLBACK;")   

def convert_to_sql_interval(time_str):
    # 정규 표현식을 사용하여 시간과 분을 추출
    pattern = r'(\d+)\s*시간\s*(\d+)\s*분'
    match = re.match(pattern, time_str)
    if match:
        hours = int(match.group(1))
        minutes = int(match.group(2))
        # SQL INTERVAL 형식으로 변환하여 반환
        return f"INTERVAL '{hours}:{minutes:02}' HOUR TO MINUTE"
    else:
        # 일치하는 패턴이 없을 경우 None 반환
        return None
    
@task
def transform(data):
    price, departure_airline, departure_layover_count, departure_duration, departure_takeoff_time, departure_landing_time,\
            arrival_layover_count, arrival_airline, arrival_duration, arrival_takeoff_time, arrival_landing_time = data
    
    # 소요시간 INTERVAL type으로
    departure_duration = convert_to_sql_interval(departure_duration)
    if arrival_duration:
        arrival_duration = convert_to_sql_interval(arrival_duration)

    # 이륙, 착륙 시간 00:00:00 형식으로
    departure_takeoff_time = f"{departure_takeoff_time}:00"
    departure_landing_time = f"{departure_landing_time}:00"
    if arrival_takeoff_time:
        arrival_takeoff_time = f"{arrival_takeoff_time}:00"
    if arrival_landing_time:
        arrival_landing_time = f"{arrival_landing_time}:00"

    return [price, departure_airline, departure_layover_count, departure_duration, departure_takeoff_time, departure_landing_time,\
            arrival_layover_count, arrival_airline, arrival_duration, arrival_takeoff_time, arrival_landing_time]

@task
def load(schema, tablename, id, data):
    logging.info("load started")
    datetime.utcnow()
    price, departure_airline, departure_layover_count, departure_duration, departure_takeoff_time, departure_landing_time,\
            arrival_layover_count, arrival_airline, arrival_duration, arrival_takeoff_time, arrival_landing_time = data
    cur = get_Redshift_connection()   

    try:
        cur.execute("BEGIN;")
        sql = f"""
            INSERT INTO {schema}.{tablename} (
                id, ts, price, departure_airline, departure_layover_count, departure_duration, departure_takeoff_time, departure_landing_time,
                arrival_layover_count, arrival_airline, arrival_duration, arrival_takeoff_time, arrival_landing_time
            ) VALUES (
                %s, GETUTCDATE(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        cur.execute(sql, (
            id, price, departure_airline, departure_layover_count, departure_duration, departure_takeoff_time, departure_landing_time,
            arrival_layover_count, arrival_airline, arrival_duration, arrival_takeoff_time, arrival_landing_time
        ))
        cur.execute("COMMIT;") 

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")


def get_flight_info():
    cursor = get_Redshift_connection()  

    cursor.execute('SELECT * FROM mool8487.flight_ticket_price_info')  # 테이블 이름 설정해야함
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
    tablename = 'flight_ticket_price_history'

    records = get_flight_info()

    for record in records:
        print(record)
        id, departure_date, departure_airport, arrival_date, arrival_airport, is_roundtrip = record
        
        data = transform(get_naver_flight_price(departure_date, departure_airport, arrival_date, arrival_airport, is_roundtrip))
        load(schema, tablename, id, data)
