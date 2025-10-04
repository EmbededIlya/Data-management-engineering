import pandas as pd
import requests
from io import StringIO

FILE_ID = "1REx1HpbXT_duA4L3jfu45RG6xynAbP1p"
file_url = f"https://drive.google.com/uc?id={FILE_ID}"

# Скачиваем файл
response = requests.get(file_url)
content = response.text

# Убираем лишние кавычки
cleaned = content.replace('"', "")

# Загружаем CSV (первая строка как заголовки)
raw_data = pd.read_csv(StringIO(cleaned), sep=",")

# Приведение типов для числовых колонок
for col in raw_data.columns[1:]:
    raw_data[col] = pd.to_numeric(raw_data[col], errors="coerce")


# Разбор "кривых" дат
def parse_date(val: str):
    months = {
        "janv": 1,
        "fev": 2,
        "mar": 3,
        "apr": 4,
        "maj": 5,
        "ijun": 6,
        "ijul": 7,
        "avg": 8,
        "sen": 9,
        "okt": 10,
        "noja": 11,
        "dek": 12,
    }
    try:
        m, y = val.split("'")
        month = months.get(m, 1)
        year = int("20" + y) if int(y) < 30 else int("19" + y)  # '05 → 2005
        return pd.Timestamp(year=year, month=month, day=1)
    except Exception:
        return pd.NaT


raw_data["Дата"] = raw_data["Дата"].map(parse_date)

# Проверка
print("Первые строки:")
print(raw_data.head(20))
print("\nТипы данных:")
print(raw_data.dtypes)

# Сохранение в Parquet ("brick")
brick_file = "dataset.brick"
raw_data.to_parquet(brick_file, engine="pyarrow", index=False)
print(f"\nДанные сохранены в {brick_file}")

# ---- Чтение обратно ----
df = pd.read_parquet(brick_file, engine="pyarrow")
print("\nПроверка чтения из brick:")
print(df.head(5))