from datetime import timedelta, datetime
from airflow import DAG
from airflow.decorators import task
from etl import fetch_rates, transform_rates, load_rates

default_args = {
    'owner': 'data_engineer',
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
        dag_id='integration_currency_etl',
        default_args=default_args,
        description='Ежедневная интеграция курсов валют (добавление)',
        schedule_interval='0 1 * * *',
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=['integration', 'currency'],
) as dag:
    @task
    def extract(**context):

        target_date = context['ds']
        print(f"Запрос данных за дату: {target_date}")

        data = fetch_rates(target_date, target_date)

        return data


    @task
    def transform(raw):
        if not raw:
            print("Нет данных для трансформации (возможно, выходной день в банке)")
            return []
        return transform_rates(raw)


    @task
    def load(transformed, **context):
        if not transformed:
            print("Пропуск загрузки: пустой список данных.")
            return


        load_rates(transformed, overwrite=False)
        print(f"Успешно загружено {len(transformed)} записей.")


    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)