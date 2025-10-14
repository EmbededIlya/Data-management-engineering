import requests
import json
from bs4 import BeautifulSoup

SOURSE = "https://www.bn.ru/analytics/"


def get_area_price_data(area_id, date_from, date_to):
    # Исправленный URL - без слеша после ID
    url = f"https://www.bn.ru/analytics/area-avg/{area_id}date_from={date_from}&date_to={date_to}"

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

    # Добавляем cookies если нужно
    cookies = {
        ".carf-frontend": "a36566028ad05d94449b10260be9babb11fdda731d05a64ec6ea07a766fe3dda%3A2%3A%7B%3A0%3Bs%3A1e%3A%22_carf-frontend%22%3B%3A1%3B:%3A32%3A%22<GUNX%8QJIIOzzz0Iee-Vn1fnnAnlin%22%3B%7B"
    }

    try:
        response = requests.get(url, headers=headers, cookies=cookies)
        print(f"Status Code: {response.status_code}")
        print(f"Response URL: {response.url}")

        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        if hasattr(e, "response") and e.response is not None:
            print(f"Response text: {e.response.text}")
        return None


# Тестируем


def main():
    html = get_area_price_data(7, "2016-09", "2025-09")
    if not html:
        return

    # data = get_area_price_data(7, "2016-09", "2025-09")
    # if data:
    #     print(json.dumps(data, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
