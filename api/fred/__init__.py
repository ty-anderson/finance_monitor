"""
FRED Bank API
-------------


"""
import os
import requests

FEDERAL_FUNDS_EFFECTIVE_RATE='FEDFUNDS'
FEDERAL_FUNDS_TARGET_RATE_UPPER='DFEDTARU'
FEDERAL_FUNDS_TARGET_RATE_LOWER='DFEDTARL'
MORTGAGE_RATE_30_YR_AVG='MORTGAGE30US'
MORTGAGE_RATE_15_YR_AVG='MORTGAGE15US'


def build_url(series_id: str, start_date: str, end_date: str) -> str:
    return (f'https://api.stlouisfed.org/fred/series/observations?'
            f'series_id={series_id}&realtime_start={start_date}&observation_start={start_date}'
            f'&realtime_end={end_date}&observation_end={end_date}'
            f'&api_key={os.getenv("FRED_API_KEY")}'
            f'&file_type=json')


def request_data(series_id: str, start_date: str, end_date: str) -> dict:
    url = build_url(series_id=series_id, start_date=start_date, end_date=end_date)
    r = requests.get(url)
    if r.status_code != 200:
        return {'status_code': r.status_code, 'message': r.text}

    return r.json()