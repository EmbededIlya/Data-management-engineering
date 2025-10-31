# Data-management-engineering
Данный репозиторий содержит в себе код, обрабатывающий следующие данные:
https://drive.google.com/drive/folders/1glbcohv_ux-CrY66wIumaUGMqwwz6b5_?usp=sharing

В данном репозитории находятся следующие папки:
- api_example: пример использования api
- data: преобразованные данные в parquet
- screenshots: снимкм экрана с выполненым заданием

Ссылка на readme c проверкой заданий: https://github.com/EmbededIlya/Data-management-engineering/blob/main/Screenshots/README.md


## Структура проекта

- **Screenshots/** - Содержит скриншоты работы проекта:
  - Parsing.png
  - Новое_переменное_окружение.png
  - Подтверждение_работы_вывод_типов_N^3.png
  - Подтверждение_работы_N^2.png
  - Подтверждение_работы_N^3.png
  - Подтверждение_работы_N^4.png
  - Подтверждение_работы_N^6.png
  - подтверждение_parsing.png

- **api_example/** - Примеры работы с API для учебных целей:
  - README.md - Документация по API примерам
  - api_bn.py - Основной модуль работы с API
  - api_example.py - Примеры использования API

- **etl/** - Основной ETL скрипт:
  - creds.loader.py - Загрузчик учетных данных
  - extract.py - Этот блок скрипта отвечает за извлечение данных и запись *.csv файлов
  - transform.py - Этот блок скрипта отвечает за преобразование типов данных и переименование колонок
  - load.py - Этот блок скрипта отвечает за загрузку данных в базу данных и запись *.parquet файлов
  - main.py - Главный блок скрипта, который читает аргументы командной строки и вызывает другие функции

## 📊 Визуализация данных

### Интерактивный просмотр:

[Cсылка nbviewer](https://nbviewer.org/github/EmbededIlya/Data-management-engineering/blob/main/notebook/EDA.ipynb)

Так как выбранная библиотека plotly имеет в себе элементы JavaScript, то Github не пропускает и не отображает корректно графики. Сделано это в целях безопасности.
Поэтому интерактивный график был сохранен отдельно:
https://github.com/EmbededIlya/Data-management-engineering/blob/main/notebook/EDA_dynamics.html

## ETL SCRIPT

NOTE: ▲ Директория `api_example` этого репозитория содержит код, который использовался для изучения работы с API. Это учебные примеры, которые показывают этапы разработки проекта. Основной ETL скрипт расположен в директории **etl** этого репозитория.

Чтобы вызвать справку скрипта, используйте:

```bash
python3 etl/main.py --help
```
Скачать только исходные данные (Extract), используйте:
```bash
python main.py ext <FILE_ID>
```
Полный ETL и запись в базу (All), используйте:
```bash
python main.py all <FILE_ID> <DB_CHOICE>