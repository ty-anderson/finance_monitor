"""
Get Federal Funds Target Rate
------------------------------

"""
import datetime
import pandas as pd
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert
from db.model import engine, fed_funds_target_tbl
from api import fred
from alert.discord import webhook_finance_monkey


def etl():
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
        webhook_finance_monkey(str(data_upper))
        return

    data_lower = fred.request_data(series_id=fred.FEDERAL_FUNDS_TARGET_RATE_LOWER,
                                   start_date=latest_date,
                                   end_date=next_30)
    dfl = pd.DataFrame(data_lower.get('observations'))
    if dfl.empty:
        print(data_lower)
        webhook_finance_monkey(str(data_lower))
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

    print('Done')


if __name__ == '__main__':
    etl()
