# Currency ETL Integration (ECB to ClickHouse)

Проект представляет собой ETL-систему на базе **Apache Airflow**, которая извлекает курсы валют (EUR/USD) из API Европейского центрального банка (ECB) и сохраняет их в **ClickHouse**.

## Архитектура
- **Python 3.11**: Основной язык разработки.
- **Apache Airflow 2.8.0**: Оркестрация задач.
- **ClickHouse**: Аналитическое хранилище данных.
- **Docker Compose**: Контейнеризация всей инфраструктуры.

## Реализованные DAGи

1.  **`maintenance_currency_etl`**:
    * **Назначение**: Ручное обслуживание и перезапись данных за произвольный период.
    * **Параметры**: Принимает `start_date` и `end_date` (формат YYYY-MM-DD).
    * **Логика**: Удаляет существующие записи в ClickHouse за указанный период и загружает новые.

2.  **`integration_currency_etl`**:
    * **Назначение**: Ежедневный инкрементальный сбор данных.
    * **Расписание**: Запускается ежедневно в 01:00 UTC.
    * **Логика**: Собирает данные за предыдущий день (`yesterday`) и добавляет их в таблицу.

## Структура данных (ClickHouse)
Таблица `currency` содержит следующие поля:
* `id` (UUID): Уникальный идентификатор записи.
* `date` (Date): Дата курса.
* `usd` (Float64): Базовое значение (1.0).
* `euro` (Float64): Значение евро по отношению к USD.
* `created` (DateTime): Время создания записи.
* `updated` (Nullable(DateTime)): Время обновления (по умолчанию NULL).

## Инструкция по запуску

### 1. Подготовка
Убедитесь, что у вас установлены `docker` и `docker-compose`.

### 2. Клонирование репозитория

```bash

git clone https://github.com/AlexanderRedzkin/ecb-currency-etl
cd ec-currency-etl


### 3. Запуск инфраструктуры
docker-compose up --build -d

Инициализация Airflow может занять 1-2 минуты.

### 4. Доступ к интерфейсам
Airflow: http://localhost:8080 (Логин: airflow, Пароль: airflow)
ClickHouse HTTP: http://localhost:8123 (Web SQL UI -> Логин: default, Пароль: clickhouse)


### 5. Зайдите в UI Airflow.

Разблокируйте (Unpause) DAG maintenance_currency_etl.
Запустите его через Trigger DAG w/ config, указав даты (например, с 2026-10-01 по 2026-10-10).

### 6. После успешного выполнения проверьте данные в ClickHouse:
(Web SQL UI -> Логин: default, Пароль: clickhouse)
SQL:

SHOW TABLES;
SELECT * FROM currency;


### Дополнительна информация:
В выходные ECB не публикует данные.


