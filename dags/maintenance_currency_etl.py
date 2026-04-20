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
    dag_id='maintenance_currency_etl',
    default_args=default_args,
    description='ETL для курсов валют за произвольный период (перезапись)',
    schedule_interval=None,           # не запускается по расписанию
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=['maintenance', 'currency'],
    params={
        'start_date': '2025-01-01',
        'end_date': '2025-01-10'
    }
) as dag:

    @task
    def extract(**context):
        start = context['params']['start_date']
        end = context['params']['end_date']
        return fetch_rates(start, end)

    @task
    def transform(raw):
        return transform_rates(raw)

    @task
    def load(transformed, **context):
        start = context['params']['start_date']
        end = context['params']['end_date']
        load_rates(transformed, overwrite=True, date_range=(start, end))

    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)