# Data-management-engineering
Данный репозиторий содержит в себе код, обрабатывающий следующие данные:
https://drive.google.com/drive/folders/1glbcohv_ux-CrY66wIumaUGMqwwz6b5_?usp=sharing

В данном репозитории находятся следующие папки:
- api_example: пример использования api
- data: преобразованные данные в parquet
- screenshots: снимкм экрана с выполненым заданием

Ссылка на readme c проверкой заданий: https://github.com/EmbededIlya/Data-management-engineering/blob/main/Screenshots/README.md

## 📊 Визуализация данных

### Интерактивный просмотр:

[Cсылка nbviewer](https://nbviewer.org/github/EmbededIlya/Data-management-engineering/blob/main/notebook/EDA.ipynb)

Так как выбранная библиотека plotly имеет в себе элементы JavaScript, то Github не пропускает и не отображает корректно графики. Сделано это в целях безопасности.
Поэтому интерактивный график был сохранен отдельно:
https://github.com/EmbededIlya/Data-management-engineering/blob/main/notebook/EDA_dynamics.html

### Использование etl:

1) Просмотр справки

Чтобы узнать, как пользоваться скриптом:
python main.py --help

Вывод покажет инструкции:
ext — только скачать сырые данные.
all — скачать, преобразовать и записать в базу.
Информация о сохранении файлов (RAW_DATA.csv и PROCESSED_DATA.parquet).

2) Скачать только исходные данные (Extract)
Команда:
python main.py ext <FILE_ID>

<FILE_ID> — идентификатор файла на Google Drive.
Данные сохраняются как RAW_DATA.csv в текущей папке.
Скрипт только скачивает и валидирует данные, без трансформации и записи в базу.

Пример:
python main.py ext 1REx1HpbXT_duA4L3jfu45RG6xynAbP1p
Вывод:
Only extracting started
Extraction finished successfully

3) Полный ETL и запись в базу (All)
Команда:
python main.py all <FILE_ID> <DB_CHOICE>

Где:
<FILE_ID> — идентификатор файла Google Drive.
<DB_CHOICE> — test или orig (выбор схемы/базы).

Что делает команда:
Extract
Скачивает данные с Google Drive (RAW_DATA.csv).

Transform
Приводит колонки к нужным типам. Обрабатывает даты. Сохраняет обработанные данные как PROCESSED_DATA.parquet.

Load
Записывает первые 100 строк (df.head(100)) в таблицу timofeev в выбранной базе PostgreSQL, добавляет первичный ключ id.

Вывод:

All stages of data transformation started. Make sure virtual env and dependencies are set up.
Processed data saved as PROCESSED_DATA.parquet
Everything has been written to the database.
=========================================================================