from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service   #드라이버는 브라우저마다 다르다
from webdriver_manager.chrome import ChromeDriverManager    # pc에 설치된 크롬과 버전을 같게 하기 위해
from selenium.webdriver.support import expected_conditions as EC

import time
import re


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
    
def naver_flight(departure_date, arrival_date, departure_airport, arrival_airport, roundtrip = True):
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")

    roundtrip_url = f"https://flight.naver.com/flights/international/{departure_airport}-{arrival_airport}-{departure_date}/"\
                    f"{arrival_airport}-{departure_airport}-{arrival_date}?adult=1&fareType=Y"
    onewaytrip_url = f"https://flight.naver.com/flights/international/{departure_airport}-{arrival_airport}-{departure_date}?adult=1&fareType=Y"

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    if roundtrip:
        driver.get(roundtrip_url)
    else:
        driver.get(onewaytrip_url)

    try:
        # 모든 정보가 로딩 되기까지 시간이 걸림. 최소값을 못 가져오는 상황을 방지
        time.sleep(10)
        # driver.implicitly_wait(10)

        # wait = WebDriverWait(driver, 60)
        
        # wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'item_num__aKbk4')))
        # print(discounted_price_element.text)
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        """
        -> 맨 첫 div를 가져와 그 안에서 클래스 안의 모든 값을 가져오도록 해야하나?
        [편도]
        div: indivisual_inner__6ST3H

        - 가격: item_num__aKbk4

        - 항공사: airline_name__0Tw5w
        - 출발 시간: route_time__xWu7a
        - 도착 시간: route_time__xWu7a
        - 경유 & 소요 시간: route_details__F_ShG -> 전처리 필요

        [왕복]
        div: concurrent_inner__OXzRp

        - 가격: item_num__aKbk4
        
        만약 출발시, 도착시 항공사가 같은 경우는 ?
        div: RoundDiffAL concurrent_item__wwwxh
        - 출발시 항공사: airline_name__0Tw5w                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
        - 출발시 출발 시간: route_time__xWu7a
        - 출발시 도착 시간: route_time__xWu7a
        - 출발시 경유 * 소요 시간: airline_name__0Tw5w -> 전처리 필요
        
        div: RoundDiffAL concurrent_item__wwwxh
        - 도착시 항공사: airline_name__0Tw5w
        - 도착시 출발 시간: route_time__xWu7a
        - 도착시 도착 시간: route_time__xWu7a
        - 도착시 경유 * 소요 시간: route_details__F_ShG -> 전처리 필요
        route_details__F_ShG
        """
        # 최저가 정보 div 찾기
        if roundtrip: #왕복
            inner_div = soup.select_one('.concurrent_inner__OXzRp')
        else:
            inner_div = soup.select_one('.indivisual_inner__6ST3H')
        # print(inner_div.prettify)

        # 가격
        price = inner_div.select_one('.item_num__aKbk4').get_text(strip=True)
        print(f"price: {price}")

        if roundtrip: # 왕복
            # 항공사
            airline = inner_div.find_all(class_='airline_name__0Tw5w')

            departure_airline = airline[0].get_text(strip=True).split(",")[0]
            if len(airline) == 1: # 출발, 도착 모두 같은 항공사일 경우
                arrival_airline = departure_airline
            else:
                arrival_airline = airline[1].get_text(strip=True).split(",")[0]
            print(departure_airline, arrival_airline)

            # 출발시 경유 횟수, 소요 시간
            layover_count = inner_div.find_all(class_='route_details__F_ShG')
            departure_layover_count, departure_duration = layover_count[0].get_text(strip=True).split(",")
            arrival_layover_count, arrival_duration = layover_count[1].get_text(strip=True).split(",")
            
            departure_layover_count = int(departure_layover_count[3:])
            departure_duration = departure_duration.strip()

            arrival_layover_count = int(arrival_layover_count[3:])
            arrival_duration = arrival_duration.strip()

            departure_duration = convert_to_sql_interval(departure_duration)
            arrival_duration = convert_to_sql_interval(arrival_duration)    

            print(departure_layover_count, departure_duration, arrival_layover_count, arrival_duration)

            # 출발시 이륙 시간 및 착륙 시간 / 도착시 이륙 시간 및
            route_times = inner_div.find_all(class_="route_time__xWu7a")

            departure_takeoff_time = route_times[0].get_text(strip=True)
            departure_landing_time = route_times[1].get_text(strip=True)

            departure_takeoff_time = f"{departure_takeoff_time}:00"
            departure_landing_time = f"{departure_landing_time}:00"

            arrival_takeoff_time = route_times[2].get_text(strip=True)
            arrival_landing_time = route_times[3].get_text(strip=True)

            arrival_takeoff_time = f"{arrival_takeoff_time}:00"
            arrival_landing_time = f"{arrival_landing_time}:00"
            print(departure_takeoff_time, departure_landing_time, arrival_takeoff_time, arrival_landing_time)
            
        else:
            # 항공사
            departure_airline = inner_div.select_one('.airline_name__0Tw5w').get_text(strip=True)
            print(departure_airline)

            # 경유 횟수, 소요 시간
            departure_layover_count, departure_duration = inner_div.select_one('.route_details__F_ShG').get_text(strip=True).split(",")

            departure_layover_count = int(departure_layover_count[3:])
            departure_duration = departure_duration.strip()

            departure_duration = convert_to_sql_interval(departure_duration)

            print(departure_layover_count, departure_duration)

            # 이륙 시간, 착륙 시간
            route_times = inner_div.find_all(class_="route_time__xWu7a") # "b",

            departure_takeoff_time = route_times[0].get_text(strip=True)
            departure_landing_time = route_times[1].get_text(strip=True)
            departure_takeoff_time = f"{departure_takeoff_time}:00"
            departure_landing_time = f"{departure_landing_time}:00"
            print(f"이륙 시간, 착륙 시간 {departure_takeoff_time, departure_landing_time}")

        # arrival_airline, arrival_layover_count, arrival_duration
        # arrival_takeoff_time, arrival_landing_time




        # if roundtrip:
        #     print(f"{departure_airport}-{arrival_airport} 왕복 비행기 최저가 : {discounted_price}")
        # else:
        #     print(f"{departure_airport}-{arrival_airport} 편도 비행기 최저가 : {discounted_price}")

    finally:
        driver.quit()



naver_flight('20240810', '20240815', 'ICN', 'SYD', roundtrip = True)


# roundtrip_url = f"https://flight.naver.com/flights/international/ICN-SYD-20240810/"\
#                 f"{arrival_airport}-{departure_airport}-{arrival_date}?adult=1&fareType=Y"
# onewaytrip_url = f"https://flight.naver.com/flights/international/ICN-SYD-20240810?adult=1&fareType=Y"

# https://flight.naver.com/flights/international/ICN-SYD-20240810/SYD-ICN-20240815?adult=1&fareType=Y
# https://flight.naver.com/flights/international/ICN-SYD-20240810?adult=1&fareType=Y