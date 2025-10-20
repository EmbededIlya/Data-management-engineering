import sqlite3
from sqlalchemy import create_engine, text, inspect
import pandas as pd

# --- Функция для загрузки кредов ---
def load_creds(db_file="creds.db"):
    conn = sqlite3.connect(db_file)
    user, pwd, url, port = conn.execute("SELECT user, pass, url, port FROM access LIMIT 1").fetchone()
    conn.close()
    return {
        "user": user,
        "pwd": pwd,
        "url": url,
        "port": str(port),
        "db": "homeworks"
    }

# --- Основной код ---
if __name__ == "__main__":
    # Загружаем креды и создаём движок
    creds = load_creds()
    engine = create_engine(f"postgresql+psycopg2://{creds['user']}:{creds['pwd']}@{creds['url']}:{creds['port']}/{creds['db']}")

    # Проверка подключения
    try:
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version();")).fetchone()
            print(f"✅ Подключение успешно: {version[0]}")
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        exit(1)

    # --- Загружаем датасет и записываем его в timofeev ---
    data_path = r"C:\Python_projects\Data-management-engineering\notebook\data\dataset.brick"
    df = pd.read_parquet(data_path).head(100)
    df.reset_index(drop=True, inplace=True)
    df.insert(0, "id", range(1, len(df)+1))  # PK
    df.to_sql("timofeev", engine, schema="public", if_exists="replace", index=False)
    print("✅ Данные записаны в таблицу public.timofeev")

    # --- Вывод списка всех таблиц ---
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema="public")
    print("\n📋 Таблицы в схеме public:")
    for t in tables:
        print(f" - {t}")

    # --- Чтение данных из timofeev ---
    df_check = pd.read_sql_table("timofeev", engine, schema="public")
    print("\n📄 Первые строки таблицы timofeev:")
    print(df_check.head())
