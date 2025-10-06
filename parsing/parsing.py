import xml.etree.ElementTree as ET
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import time
import wget
from tqdm.notebook import tqdm
import requests
import os

# --- Настройки ---
URL = "https://www.bn.ru/analytics/"
OUTPUT_FILENAME = "data/cost_apartment.csv"
CHROMEDRIVER_PATH = "C:\chromedriver-win64\chromedriver-win64/chromedriver.exe"  # укажи путь к скачанному ChromeDriver

# Создаём папку для CSV, если её нет
os.makedirs(os.path.dirname(OUTPUT_FILENAME), exist_ok=True)

# --- Настройка Selenium ---
options = Options()
options.headless = True  # запуск без окна браузера

service = Service(CHROMEDRIVER_PATH)
driver = webdriver.Chrome(service=service, options=options)

try:
    print("Открываем страницу...")
    driver.get(URL)
    time.sleep(5)  # ждём загрузки JS

    # Получаем весь HTML после рендера
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    # Находим статьи через BeautifulSoup
    articles = soup.select(".analytics-item")
    data = []

    for a in articles:
        title_elem = a.select_one(".analytics-item__title a")
        date_elem = a.select_one(".analytics-item__date")
        if title_elem and date_elem:
            data.append({
                "title": title_elem.get_text(strip=True),
                "date": date_elem.get_text(strip=True),
                "link": title_elem.get("href")
            })

    # Сохраняем в CSV
    df = pd.DataFrame(data)
    df.to_csv(OUTPUT_FILENAME, index=False)
    print(f"Собрано {len(df)} статей. Данные сохранены в {OUTPUT_FILENAME}")

finally:
    driver.quit()