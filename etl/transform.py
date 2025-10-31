import pandas as pd


def transform_data(df: pd.DataFrame, brick_path: str = "dataset.brick") -> pd.DataFrame:
    """
    Приводит числовые колонки к числовому типу, парсит даты и сохраняет в parquet.
    Возвращает преобразованный DataFrame.
    """

    # Парсинг дат
    def parse_date(val: str) -> pd.Timestamp:
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
            year = int("20" + y) if int(y) < 30 else int("19" + y)
            return pd.Timestamp(year=year, month=month, day=1)
        except Exception:
            return pd.NaT

    # Преобразование числовых колонок
    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if "Дата" in df.columns:
        df["Дата"] = df["Дата"].map(parse_date)

    # Сохранение в parquet
    df.to_parquet(brick_path, engine="pyarrow", index=False)
    return df
