import os
import pandas as pd
import requests
from io import StringIO

RAW_DIR = "data/raw"
os.makedirs(RAW_DIR, exist_ok=True)


def extract_data(file_id: str, save_name: str = "dataset.csv") -> pd.DataFrame:
    """
    Скачивает CSV из Google Drive, убирает лишние кавычки, сохраняет в data/raw,
    и возвращает загруженный DataFrame.
    """
    # Скачивание
    file_url = f"https://drive.google.com/uc?id={file_id}"
    response = requests.get(file_url)
    content = response.text
    cleaned = content.replace('"', "")

    # Сохранение CSV
    save_path = os.path.join(RAW_DIR, save_name)
    with open(save_path, "w", encoding="utf-8") as f:
        f.write(cleaned)

    # Загрузка CSV
    df = pd.read_csv(save_path, sep=",")
    return df
