# Проект: ETL курсов валют (EUR/USD) из API ECB


# Запуск проекта:

1. Клонируйте репозиторий:
git clone https://github.com/AlexanderRedzkin/ecb-currency-etl.git
cd ecb-currency-etl

2. Создайте файл .env с содержимым AIRFLOW_UID=50000 или напишите в терминале (macOS/Linux):
echo -e "AIRFLOW_UID=$(id -u)" > .env   

3. Запустите контейнеры:
docker compose up -d --build

4. Дождитесь, пока все сервисы поднимутся (проверьте командой docker compose ps).
Airflow Web UI будет доступен по адресу http://localhost:8080.
Логин/пароль: airflow/airflow.