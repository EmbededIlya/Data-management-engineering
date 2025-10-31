import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
import pandas as pd

# --- Функция для загрузки кредов ---
def load_creds(env_file=".env"):
    # Загружаем переменные окружения из .env
    load_dotenv(env_file)

    creds = {
        "user": os.getenv("DB_USER"),
        "pwd": os.getenv("DB_PASS"),
        "url": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT", "5432"),
        "db": os.getenv("DB_NAME", "homeworks"),
    }

    # Проверяем, что все ключи заданы
    missing = [k for k, v in creds.items() if not v]
    if missing:
        raise ValueError(f"❌ Отсутствуют переменные окружения: {', '.join(missing)}")

    return creds


# --- Основной код ---
if __name__ == "__main__":
    # Загружаем креды и создаём движок
    creds = load_creds()
    conn_str = f"postgresql+psycopg2://{creds['user']}:{creds['pwd']}@{creds['url']}:{creds['port']}/{creds['db']}"
    engine = create_engine(conn_str)

    # --- Проверка подключения ---
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
    df.insert(0, "id", range(1, len(df) + 1))  # добавляем PK
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
