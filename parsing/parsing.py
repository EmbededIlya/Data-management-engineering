import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE = "https://www.bn.ru/analytics/"

def fetch(url):
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.text

def extract_urls(text, base_url):
    urls = set()
    for part in text.split('"'):
        if part.startswith("http") or part.startswith("/"):
            urls.add(urljoin(base_url, part))
    return urls

def main():
    html = fetch(BASE)
    soup = BeautifulSoup(html, "html.parser")

    candidate_urls = set()

    # Inline скрипты
    for s in soup.find_all("script"):
        if s.string:
            candidate_urls.update(extract_urls(s.string, BASE))
    print("\n Inline скрипты")    
    for u in sorted(candidate_urls):
        print(u)

    # Внешние скрипты
    for s in soup.find_all("script", src=True):
        try:
            candidate_urls.update(extract_urls(fetch(urljoin(BASE, s["src"])), BASE))
        except Exception:
            pass
    print("\n Внешние скрипты")    
    for u in sorted(candidate_urls):
        print(u)

    # Добавляем типичные endpoint'ы - пришлось залезть на сайт и посмотреть запросы
    typical_endpoints = [
        urljoin(BASE, "area-list/"),
        urljoin(BASE, "area-list"),
        urljoin(BASE, "area-avg/"),
        urljoin(BASE, "area-avg/?date_from=2016-09&date_to=2025-09")
    ]
    candidate_urls.update(typical_endpoints)
    print("\n Добавляем типичные endpoint'ы")    
    for u in sorted(candidate_urls):
        print(u)


if __name__ == "__main__":
    main()

