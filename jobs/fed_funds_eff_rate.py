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
from alert.discord import webhook_finance_monkey


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

    if len(new_records) > 0:
        rates = get_tx(years_back=2)
        rates = rates.sort_values(by=['date'])
        df_str = format_pd(rates)
        webhook_finance_monkey(f'New Federal Funds Effective Rate is out.\n'
                               f'Here is the T24 month rates:\n\n\n'
                               + df_str)


def format_pd(df):
    df_str = df.to_string(index=False, header=False)

    # Format columns with pipes
    columns = '| ' + ' | '.join(df.columns) + ' |'

    # Format each row with pipes
    rows = df_str.replace(' ', ' | ')

    # Ensure every row ends with a pipe
    formatted_rows = '\n'.join(['| ' + row + '% |' for row in rows.split('\n')])

    # Combine the formatted header and formatted rows
    formatted_df = f"{columns}\n{formatted_rows}"
    return formatted_df


if __name__ == '__main__':
    etl()
