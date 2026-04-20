from datetime import timedelta
from airflow import DAG
from airflow.decorators import task
import pendulum
from etl import fetch_rates, transform_rates, load_rates

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='integration_currency_etl',
    default_args=default_args,
    description='Ежедневная интеграция курсов валют (добавление)',
    schedule_interval='0 1 * * *',      # каждый день в 01:00
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=['integration', 'currency'],
) as dag:

    @task
    def extract(**context):
        # ds – дата запуска в формате YYYY-MM-DD (по умолчанию предыдущий день)
        target_date = context['ds']
        return fetch_rates(target_date, target_date)

    @task
    def transform(raw):
        return transform_rates(raw)

    @task
    def load(transformed):
        load_rates(transformed, overwrite=False)

    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)