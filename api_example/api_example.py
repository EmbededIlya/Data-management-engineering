import requests
from tqdm import tqdm
import pandas as pd
import xml.etree.ElementTree as ET

API_URL = "http://www.cbr.ru/scripts/XML_dynamic.asp"
OUTPUT_FILENAME = "data/currencies.csv"

CURRENCIES = {
    "USD": "R01235",  # доллар США
    "EUR": "R01239",  # евро
    "CNY": "R01375"   # юань
}


def load_currency(api_url: str, params: dict, code: str) -> pd.DataFrame:
    """Загрузить одну валюту и вернуть DataFrame"""
    response = requests.get(api_url, params=params)

    if response.status_code != 200:
        print(f"WARNING! {code}: status {response.status_code}")
        return pd.DataFrame()

    root = ET.fromstring(response.text)

    data = []
    for record in root.findall("Record"):
        data.append({
            "date": record.attrib.get("Date"),
            "currency": code,
            "value": record.find("Value").text if record.find("Value") is not None else None
        })

    df = pd.DataFrame(data)
    if not df.empty:
        df["value"] = df["value"].str.replace(",", ".").astype(float)
        df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y")

    return df


def main():
    date_params = {
        "date_req1": "02/01/2022",
        "date_req2": "20/12/2022",
    }

    results = []
    for code, val_id in tqdm(CURRENCIES.items(), desc="Загрузка валют"):
        params = {**date_params, "VAL_NM_RQ": val_id}
        df = load_currency(API_URL, params, code)
        if not df.empty:
            results.append(df)

    if results:
        final_df = pd.concat(results, ignore_index=True)
        final_df.to_csv(OUTPUT_FILENAME, index=False)
        print(final_df.info())
        print(final_df.head(10))
    else:
        print("Не удалось получить данные")


if __name__ == "__main__":
    main()
