import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service   #드라이버는 브라우저마다 다르다
from webdriver_manager.chrome import ChromeDriverManager    # pc에 설치된 크롬과 버전을 같게 하기 위해
from selenium.webdriver.common.by import By     # 응답 요소에서 특정 요소를 추출하는 메서드
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def coupang_price(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        price_element = soup.find('div', class_='prod-coupon-price price-align major-price-coupon').select_one('.total-price strong')
        if price_element:
            price = price_element.get_text(strip=True)
            print(f"쿠팡 현재 가격 : {price}")
        else:
            price_element = soup.find('div', class_='prod-coupon-price price-align major-price-coupon').select_one('.total-price strong')
            if price_element:
                price = price_element.get_text(strip=True)
                print(f"쿠팡 현재 가격 : {price}")
            else:
                print("URL을 다시 입력해주세요")
    else:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")

coupang_price("https://link.coupang.com/a/bEPwcW")


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
        wait = WebDriverWait(driver, 30)
        discounted_price_element = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'item_num__aKbk4')))

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        discounted_price = soup.select_one('.item_num__aKbk4').get_text(strip=True)
        if roundtrip:
            print(f"{departure_airport}-{arrival_airport} 왕복 비행기 최저가 : {discounted_price}")
        else:
            print(f"{departure_airport}-{arrival_airport} 편도 비행기 최저가 : {discounted_price}")

    finally:
        driver.quit()

naver_flight('20240810', '20240815', 'ICN', 'SYD', roundtrip = True)