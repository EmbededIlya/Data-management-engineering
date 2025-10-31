import os
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()

def load_creds():
    return {
        "user": os.getenv("DB_USER"),
        "pwd": os.getenv("DB_PASSWORD"),
        "url": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT", "5432"),
        "db": os.getenv("DB_NAME")
    }