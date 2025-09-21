import pandas as pd

FILE_ID = "1pEpYcdiSg4bxPiJDBAZoRReWXLQI7FCU"  # замените на свой ID
file_url = f"https://drive.google.com/uc?id={FILE_ID}"

# читаем CSV с Google Drive
raw_data = pd.read_csv(file_url)

# выводим первые 10 строк
print(raw_data.head(10))