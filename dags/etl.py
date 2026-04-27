import uuid
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, Any
from clickhouse_driver import Client
import requests
import xml.etree.ElementTree as ET
from datetime import datetime


CH_HOST = 'clickhouse'
CH_PORT = 9000
CH_DATABASE = 'default'
CH_TABLE = 'currency'




def fetch_rates(start_date, end_date):
    url = f"https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?startPeriod={start_date}&endPeriod={end_date}"

    headers = {"Accept": "application/vnd.sdmx.genericdata+xml;version=2.1"}

    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
    except Exception as e:
        print(f"Ошибка при запросе к ECB: {e}")
        return []

    root = ET.fromstring(response.content)
    rates = []

    # Пространства имен в XML от ECB
    ns = {
        'generic': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic',
    }

    # Ищем все элементы Observation (Obs)
    for obs in root.findall('.//generic:Obs', ns):
        # В формате Generic Data дата лежит в ObsDimension, а значение в ObsValue
        obs_dim = obs.find('generic:ObsDimension', ns)
        obs_val = obs.find('generic:ObsValue', ns)

        if obs_dim is not None and obs_val is not None:
            date_str = obs_dim.get('value')
            value_str = obs_val.get('value')

            try:
                rates.append({
                    'date': datetime.strptime(date_str, '%Y-%m-%d').date(),
                    'euro': float(value_str)
                })
            except (ValueError, TypeError):
                continue

    return rates


def transform_rates(raw_rates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

    now = datetime.now()
    transformed = []
    for item in raw_rates:
        transformed.append({
            'id': str(uuid.uuid4()),
            'date': item['date'],
            'usd': 1.0,
            'euro': item['euro'],
            'created': now,
            'updated': None
        })
    print(f"[DEBUG] transform_rates: получено {len(transformed)} записей")
    return transformed


def load_rates(data: List[Dict[str, Any]], overwrite: bool = False, date_range: tuple = None):
    """
    Загружает данные в ClickHouse.
    Если overwrite=True и date_range=(start_date, end_date), то сначала удаляет записи за этот период.
    """
    client = Client(
        host='clickhouse',
        port=9000,
        database='default',
        user='default',
        password='clickhouse'
    )

    if overwrite and date_range:
        start_date, end_date = date_range
        delete_query = f"DELETE FROM {CH_TABLE} WHERE date >= %(start)s AND date <= %(end)s"
        client.execute(delete_query, {'start': start_date, 'end': end_date})

    if not data:
        print("Нет данных для вставки")
        return

    insert_query = f"""
        INSERT INTO {CH_TABLE} (id, date, usd, euro, created, updated)
        VALUES
    """
    client.execute(insert_query, data)
    print(f"Загружено {len(data)} записей в таблицу {CH_TABLE}.")