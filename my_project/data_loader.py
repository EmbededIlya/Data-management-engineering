import pandas as pd

FILE_ID = "1REx1HpbXT_duA4L3jfu45RG6xynAbP1p"  
file_url = f"https://drive.google.com/uc?id={FILE_ID}"

# читаем CSV с Google Drive
raw_data = pd.read_csv(file_url)

# выводим первые 10 строк
print(raw_data.head(10))