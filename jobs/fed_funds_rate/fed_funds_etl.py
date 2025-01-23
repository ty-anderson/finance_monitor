"""
Get Federal Funds Effective Rate ETL
------------------------------------

Download the API data for Federal Funds Rate and Federal Funds Target Rate.
If there's any new data, load to database and then report it via Discord Webhook
"""
import datetime
import pandas as pd
import requests
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from api import fred
from decorators import try_alert
from alert.discord import send_alert
from db.model import engine, fed_funds_eff_rate_tbl, fed_funds_target_tbl

load_dotenv()


@try_alert
def fed_funds_effective_rate_etl():
    print('Checking for todays effective rate')
    today = datetime.date.today()
    url = 'https://www.federalreserve.gov/feeds/Data/H15_H15_RIFSPFF_N.B.XML'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "xml")

    data = {
        'date': soup.item.statistics.otherStatistic.observationPeriod.string,
        'value': soup.item.statistics.otherStatistic.value.string,
        'id': str(soup.item.statistics.otherStatistic.observationPeriod.string).replace('-', ''),
        'carry_forward': False
    }

    with engine.connect() as conn:
        today_reported_date = datetime.datetime.strptime(data['date'], '%Y-%m-%d').date()

        if today_reported_date < today or data['value'] == 'ND':
            # if today doesn't have a reported number, carry the previous number forward
            most_recent_val = select(func.max(fed_funds_eff_rate_tbl.c.date))
            most_recent_val = conn.execute(most_recent_val).fetchone()[0]
            data['date'] = datetime.datetime.strftime(most_recent_val + datetime.timedelta(days=1), '%Y-%m-%d')
            data['id'] = int(data['id']) + 1
            data['carry_forward'] = True

        insert_stmt = insert(fed_funds_eff_rate_tbl).values(data)
        insert_stmt = insert_stmt.on_conflict_do_nothing()
        conn.execute(insert_stmt)

        conn.commit()


@try_alert
def fed_funds_target_rate_etl():
    start_time = datetime.datetime.now()
    with engine.connect() as conn:
        latest_date = conn.execute(
            select(func.coalesce(func.max(fed_funds_target_tbl.c.date), '2020-01-01'))).fetchone()

    latest_date = latest_date[0] + datetime.timedelta(days=1)
    if latest_date > start_time.date():
        print('no new data')
        return

    next_30 = min(latest_date + datetime.timedelta(days=30), start_time.date())
    print(f'Pulling from {latest_date} to {next_30}')
    data_upper = fred.request_data(series_id=fred.FEDERAL_FUNDS_TARGET_RATE_UPPER,
                                   start_date=latest_date,
                                   end_date=next_30)
    dfu = pd.DataFrame(data_upper.get('observations'))
    if dfu.empty:
        print(data_upper)
        send_alert(str(data_upper),
                   channel='Finance Monkey')
        return

    data_lower = fred.request_data(series_id=fred.FEDERAL_FUNDS_TARGET_RATE_LOWER,
                                   start_date=latest_date,
                                   end_date=next_30)
    dfl = pd.DataFrame(data_lower.get('observations'))
    if dfl.empty:
        print(data_lower)
        send_alert(str(data_lower),
                   channel='Finance Monkey')
        return

    dfu = dfu[['date', 'value']].rename(columns={'value': 'value_upper'})
    dfl = dfl[['date', 'value']].rename(columns={'value': 'value_lower'})
    df = dfu.merge(dfl, on='date', how='left')
    df['date'] = pd.to_datetime(df['date'])
    df['id'] = df['date'].dt.strftime('%Y%m%d')
    df['date'] = df['date'].dt.date
    df['id'] = df['id'].astype(int)

    with engine.connect() as conn:
        insert_stmt = insert(fed_funds_target_tbl).values(df.to_dict(orient='records'))
        insert_stmt = insert_stmt.on_conflict_do_nothing()
        conn.execute(insert_stmt)
        conn.commit()


def main():
    fed_funds_effective_rate_etl()
    fed_funds_target_rate_etl()


if __name__ == '__main__':
    main()
