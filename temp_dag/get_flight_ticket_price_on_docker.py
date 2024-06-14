from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from selenium import webdriver
from selenium.webdriver.chrome.service import Service   #드라이버는 브라우저마다 다르다
from webdriver_manager.chrome import ChromeDriverManager    # pc에 설치된 크롬과 버전을 같게 하기 위해
from bs4 import BeautifulSoup

import re

from datetime import datetime
from datetime import timedelta

import logging

import time



def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

# info 테이블에서 항공권 정보 가져오기
def get_flight_ticket_info(**context): # schema, infotablename
    logging.info(datetime.utcnow())
    logging.info(f"start get records flight ticket info")

    schema = context["params"]["schema"]
    infotablename = context["params"]["infotablename"]

    try:
        cursor = get_Redshift_connection()  

        # 데이터 추출, 과거가 된 데이터는 제외
        cursor.execute(f'SELECT * FROM {schema}.{infotablename} WHERE departure_date > CURRENT_DATE;')
        records = cursor.fetchall()

        cursor.close()

        logging.info(f"get records flight ticker info done")
        return records
    
    except Exception as e:
        logging.error(f"Error in get_flight_ticket_info task: {e}")
        raise
    

# 항공권 별 최저가 추출
def get_flight_ticket_price(**context):
    logging.info(f"start get_flight_ticket_price")

    ticket_info = context['task_instance'].xcom_pull(key="return_value", task_ids='get_flight_ticket_info')

    prices = []
    for record in ticket_info:
        id, departure_date, departure_airport, arrival_date, arrival_airport, roundtrip = record
        logging.info(f"flight ticket id: {id}")

        # url에 삽입 가능하도록 날짜 포맷 변경
        departure_date = departure_date.strftime('%Y%m%d')
        if roundtrip: arrival_date = arrival_date.strftime('%Y%m%d')

        logging.info(f"출발일: {departure_date}, 출발지: {departure_airport}, 도착일: {arrival_date}, 도착지: {arrival_airport}, 왕복 여부: {roundtrip}")

        options = webdriver.ChromeOptions()
        user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.4.1.6 Safari/537.36"
        options.add_argument('user-agent=' + user_agent)
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        remote_webdriver = 'remote_chromedriver'
        with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options) as driver: # docker 환경에서 실행할 경우
            # Scraping part
            # driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options) # docker에서는 필요 없음

            if roundtrip: # 왕복
                url = f"https://flight.naver.com/flights/international/{departure_airport}-{arrival_airport}-{departure_date}/"\
                            f"{arrival_airport}-{departure_airport}-{arrival_date}?adult=1&fareType=Y"
            else:  # 편도
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
                else: # 편도
                    inner_div = soup.select_one('.indivisual_inner__6ST3H')
                # print(inner_div.prettify())

                # 가격
                price = inner_div.select_one('.item_num__aKbk4').get_text(strip=True)
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
                    route_times = inner_div.find_all(class_="route_time__xWu7a")
                    departure_takeoff_time = route_times[0].text
                    departure_landing_time = route_times[1].text

                    logging.info(f"출발시 이륙 시간 : {departure_takeoff_time}, 출발시 착륙 시간: {departure_landing_time}")

                    # 편도일 경우 Null 값이 되는 컬럼
                    arrival_layover_count, arrival_airline, arrival_duration, arrival_takeoff_time, arrival_landing_time = None, None, None, None, None

                price = [id, price, departure_airline, departure_layover_count, departure_duration, departure_takeoff_time, departure_landing_time,\
                        arrival_layover_count, arrival_airline, arrival_duration, arrival_takeoff_time, arrival_landing_time]
            
                prices.append(price)

            except Exception as error:
                logging.error(error)
                raise

    # try:
    #     driver.quit()
    # except Exception as error:
    #     logging.error(error)
    #     raise

    logging.info("get price done")
    
    try:
        return prices
    except Exception as e:
        logging.error(f"Error in get_flight_ticket_price task: {e}")
        raise

# INTERVAL 컬럼 insert 형식 맞추기: "h시간 m분" -> "h hours m minutes" 형식으로 변경
def convert_to_sql_interval(time_str):
    # 정규 표현식을 사용하여 시간과 분을 추출
    pattern = r'(\d+)\s*시간\s*(\d+)\s*분'
    match = re.match(pattern, time_str)
    if match:
        hours = int(match.group(1))
        minutes = int(match.group(2))
        # SQL INTERVAL 형식으로 변환하여 반환
        return f"{hours} hours {minutes} minutes"
    else:
        # 일치하는 패턴이 없을 경우 None 반환
        return None

# 데이터 형식 변환
def transform_format(**context):
    logging.info("transform started")
    
    data = context['task_instance'].xcom_pull(key="return_value", task_ids='get_flight_ticket_price')

    prices = []
    for row in data:
        try:
            id, price, departure_airline, departure_layover_count, departure_duration, departure_takeoff_time, departure_landing_time,\
                    arrival_layover_count, arrival_airline, arrival_duration, arrival_takeoff_time, arrival_landing_time = row
            
            logging.info("flight ticket id: {id}")
            
            # 가격 정수형으로
            price = int(price.replace(",", ""))
            logging.info("transform price : {price}")

            # 소요시간 INTERVAL type으로
            departure_duration = convert_to_sql_interval(departure_duration)
            if arrival_duration:
                arrival_duration = convert_to_sql_interval(arrival_duration)

            logging.info("transform departure_duration : {departure_duration}, arrival_duration : {arrival_duration}")

            # 이륙, 착륙 시간 00:00:00 형식으로
            departure_takeoff_time = f"{departure_takeoff_time}:00"
            departure_landing_time = f"{departure_landing_time}:00"
            if arrival_takeoff_time:
                arrival_takeoff_time = f"{arrival_takeoff_time}:00"
            if arrival_landing_time:
                arrival_landing_time = f"{arrival_landing_time}:00"

            logging.info("transform departure_takeoff_time : {departure_takeoff_time}, departure_landing_time : {departure_landing_time}")
            logging.info("transform arrival_takeoff_time : {arrival_takeoff_time}, arrival_landing_time : {arrival_landing_time}")

            price = [id, price, departure_airline, departure_layover_count, departure_duration, departure_takeoff_time, departure_landing_time,\
                            arrival_layover_count, arrival_airline, arrival_duration, arrival_takeoff_time, arrival_landing_time]
                
            prices.append(price)
        
        except Exception as error:
            logging.error(error)
            raise

    logging.info("transform done")
    try:
        return prices
    except Exception as e:
        logging.error(f"Error in transform_format task: {e}")
        raise


def load(**context):
    logging.info("load started")

    schema = context["params"]["schema"]
    pricetablename = context["params"]["pricetablename"]
    data = context['task_instance'].xcom_pull(key="return_value", task_ids='transform_format')
    execution_date = context["execution_date"].strftime('%Y-%m-%d %H:%M:%S')

    cur = get_Redshift_connection()   
    
    for row in data:
        id, price, departure_airline, departure_layover_count, departure_duration, departure_takeoff_time, departure_landing_time,\
                arrival_layover_count, arrival_airline, arrival_duration, arrival_takeoff_time, arrival_landing_time = row
        
        logging.info("flight ticket id: {id}")

        try:
            cur.execute("BEGIN;")
            sql = f"""
                INSERT INTO {schema}.{pricetablename} (
                    id, ts, price, departure_airline, departure_layover_count, departure_duration, departure_takeoff_time, departure_landing_time,
                    arrival_layover_count, arrival_airline, arrival_duration, arrival_takeoff_time, arrival_landing_time
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            cur.execute(sql, (
                id, execution_date, price, departure_airline, departure_layover_count, departure_duration, departure_takeoff_time, departure_landing_time,
                arrival_layover_count, arrival_airline, arrival_duration, arrival_takeoff_time, arrival_landing_time
            ))
            cur.execute("COMMIT;") 

        except Exception as error:
            logging.error(error)
            logging.info("ROLLBACK")
            cur.execute("ROLLBACK;")
            raise

    logging.info("load done")



with DAG(
    dag_id='get_naver_flight_price_on_docker',
    start_date=datetime(2024, 6, 1),  # 날짜가 미래인 경우 실행이 안됨
    schedule='0 0 * * *', # 매일 자정
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 0,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:
    schema = 'mool8487'   ## 자신의 스키마로 변경
    infotablename = 'flight_ticket_info'
    pricetablename = 'flight_ticket_price_history'

    get_flight_ticket_info = PythonOperator(
        task_id = 'get_flight_ticket_info',
        python_callable = get_flight_ticket_info,
        params = {
            'schema':  schema,
            'infotablename': infotablename
        })

    get_flight_ticket_price = PythonOperator(
        task_id = 'get_flight_ticket_price',
        python_callable = get_flight_ticket_price)

    transform_format = PythonOperator(
        task_id = 'transform_format',
        python_callable = transform_format)
    
    load = PythonOperator(
        task_id = 'load',
        python_callable = load,
        params = {
            'schema':  schema,
            'pricetablename': pricetablename
        })
    
    get_flight_ticket_info >> get_flight_ticket_price >> transform_format >> load