import uuid
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, Any
from clickhouse_driver import Client



CH_HOST = 'clickhouse'
CH_PORT = 9000
CH_DATABASE = 'default'
CH_TABLE = 'currency'

def fetch_rates(start_date: str, end_date: str) -> List[Dict[str, Any]]:

    base_url = "https://data-api.ecb.europa.eu/service/data/EXR/"
    series_key = "D.USD.EUR.SP00.A"
    url = f"{base_url}{series_key}?startPeriod={start_date}&endPeriod={end_date}"
    headers = {"Accept": "application/vnd.sdmx.structurespecificdata+xml;version=2.1"}

    response = requests.get(url, headers=headers, verify=False)
    response.raise_for_status()

    root = ET.fromstring(response.content)
    namespaces = {
        'generic': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic',
        'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message'
    }

    rates = []

    for obs in root.findall('.//generic:Obs', namespaces):
        time_dim = obs.find('.//generic:ObsDimension', namespaces)
        date_str = time_dim.attrib.get('value') if time_dim is not None else None
        obs_value = obs.find('.//generic:ObsValue', namespaces)
        value = obs_value.attrib.get('value') if obs_value is not None else None

        if date_str and value:
            try:
                rates.append({
                    'date': datetime.strptime(date_str, '%Y-%m-%d').date(),
                    'euro': float(value)
                })
            except ValueError:
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
    return transformed


def load_rates(data: List[Dict[str, Any]], overwrite: bool = False, date_range: tuple = None):
    """
    Загружает данные в ClickHouse.
    Если overwrite=True и date_range=(start_date, end_date), то сначала удаляет записи за этот период.
    """
    client = Client(host=CH_HOST, port=CH_PORT, database=CH_DATABASE)

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