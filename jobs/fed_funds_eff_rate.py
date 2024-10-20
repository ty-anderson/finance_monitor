"""
Get Federal Funds Effective Rate
--------------------------------

Download the API data for Federal Funds Rate and Federal Funds Target Rate.
If there's any new data, load to database and then report it via Discord Webhook
"""
import load_env
import datetime
import pandas as pd
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert
from db.model import engine, fed_funds_eff_rate_tbl, fed_funds_target_tbl
from api import fred
from alert.discord import webhook_finance_monkey
from jobs.funcs import format_table_with_pipes, plot_df_data
from jobs.fed_funds_target_rate import etl as target_etl


def get_reporting_data(years_back: int):
    today = datetime.date.today()
    min_date = datetime.date(today.year - years_back, today.month, 1)

    with engine.connect() as conn:
        stmt = select(fed_funds_eff_rate_tbl.c.date,
                      fed_funds_eff_rate_tbl.c.value).where(fed_funds_eff_rate_tbl.c.date > min_date)
        df = pd.read_sql(stmt, conn)

        target_stmt = select(fed_funds_target_tbl.c.date,
                             fed_funds_target_tbl.c.value_upper,
                             fed_funds_target_tbl.c.value_lower).where(fed_funds_target_tbl.c.date > (today - datetime.timedelta(days=10)))
        df_target = pd.read_sql(target_stmt, conn)


    return df, df_target


def etl():
    start_time = datetime.datetime.now()
    with engine.connect() as conn:
        latest_date = conn.execute(
            select(func.coalesce(func.max(fed_funds_eff_rate_tbl.c.date), '1800-01-01'))).fetchone()

    data = fred.request_data(series_id=fred.FEDERAL_FUNDS_EFFECTIVE_RATE,
                             start_date=latest_date[0],
                             end_date=start_time.strftime('%Y-%m-%d'))
    df = pd.DataFrame(data.get('observations'))
    df = df[['date', 'value']]
    if df.empty:
        print('Nothing to report')
        return

    df['date'] = pd.to_datetime(df['date'])
    df['id'] = df['date'].dt.strftime('%Y%m%d')
    df['date'] = df['date'].dt.date
    df['id'] = df['id'].astype(int)

    with engine.connect() as conn:
        insert_stmt = insert(fed_funds_eff_rate_tbl).values(df.to_dict(orient='records'))
        insert_stmt = insert_stmt.on_conflict_do_nothing().returning(fed_funds_eff_rate_tbl.c.date)
        result = conn.execute(insert_stmt)
        new_records = result.fetchall()
        conn.commit()

    if len(new_records) == 0:
        rates, target_rates = get_reporting_data(years_back=2)
        rates = rates.sort_values(by=['date'])
        target_rates = target_rates.sort_values(by=['date'])
        df_str = format_table_with_pipes(rates.astype(str))
        target_str = format_table_with_pipes(target_rates.astype(str))

        img_buffer = plot_df_data(rates)
        webhook_finance_monkey(f'New Federal Funds Effective Rate is out.\n'
                               f'Here is the T24 month rates:\n\n\n'
                               + df_str + '\n\n' + target_str,
                               file=('plot.png', img_buffer, 'image/png'))
        img_buffer.close()


if __name__ == '__main__':
    target_etl()
    etl()
