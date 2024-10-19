"""
Get Federal Funds Effective Rate
--------------------------------


"""
import datetime
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert
from db.model import engine, fed_funds_eff_rate_tbl
from api import fred


def get_tx(years_back: int):
    today = datetime.date.today()
    t12_date = datetime.date(today.year - years_back, today.month, 1)

    with engine.connect() as conn:
        stmt = select(fed_funds_eff_rate_tbl.c.date,
                      fed_funds_eff_rate_tbl.c.value).where(fed_funds_eff_rate_tbl.c.date > t12_date)
        df = pd.read_sql(stmt, conn)

    return df


def report(df):
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    # Plot the data
    ax = df.plot(y='value', kind='line', marker='o')

    # Add data labels
    for i, value in enumerate(df['value']):
        ax.annotate(f'{value}%', (df.index[i], value), textcoords="offset points", xytext=(0, 5), ha='center')

    plt.show()


def etl():
    with engine.connect() as conn:
        latest_date = conn.execute(
            select(func.coalesce(func.max(fed_funds_eff_rate_tbl.c.date), '1800-01-01'))).fetchone()

    data = fred.request_data(series_id=fred.FEDERAL_FUNDS_EFFECTIVE_RATE, latest_date=latest_date[0])
    df = pd.DataFrame(data.get('observations'))
    df = df[['date', 'value']]
    if df.empty:
        print('Nothing to report')
        return

    df['date'] = pd.to_datetime(df['date'])
    df['id'] = df['date'].dt.strftime('%Y%m%d')
    df['date'] = df['date'].dt.date
    df['id'] = df['id'].astype(int)
    df = df[df['date'] > latest_date[0]]
    if df.empty:
        print('Nothing new to report')
        return

    with engine.connect() as conn:
        insert_stmt = insert(fed_funds_eff_rate_tbl).values(df.to_dict(orient='records'))
        insert_stmt = insert_stmt.on_conflict_do_nothing()
        conn.execute(insert_stmt)
        conn.commit()


if __name__ == '__main__':
    etl()
    rates = get_tx(years_back=2)
    report(rates)
