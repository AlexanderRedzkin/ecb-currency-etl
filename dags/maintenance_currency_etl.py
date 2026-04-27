from datetime import timedelta, datetime
from airflow import DAG
from airflow.decorators import task
from etl import fetch_rates, transform_rates, load_rates

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='maintenance_currency_etl',
        default_args=default_args,
        description='ETL для курсов валют за произвольный период (перезапись)',
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=['maintenance', 'currency'],
        # Эти параметры будут видны в интерфейсе Airflow при запуске "Trigger DAG w/ config"
        params={
            'start_date': '2026-01-01',
            'end_date': '2026-01-10'
        }
) as dag:
    @task
    def extract(**kwargs):
        # Достаем параметры из kwargs['params']
        params = kwargs.get('params', {})
        start = params.get('start_date')
        end = params.get('end_date')

        print(f"Запуск извлечения данных с {start} по {end}")

        data = fetch_rates(start, end)

        print(f"Получено записей: {len(data)}")
        return data


    @task
    def transform(raw):
        if not raw:
            print("Предупреждение: Данные для трансформации пусты!")
            return []
        return transform_rates(raw)


    @task
    def load(transformed, **kwargs):
        params = kwargs.get('params', {})
        start = params.get('start_date')
        end = params.get('end_date')

        if not transformed:
            print("Загрузка отменена: нет данных.")
            return

        load_rates(transformed, overwrite=True, date_range=(start, end))
        print("Данные успешно загружены.")


    # Соединяем задачи
    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)
