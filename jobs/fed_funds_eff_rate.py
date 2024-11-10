"""
Get Federal Funds Effective Rate
--------------------------------

Download the API data for Federal Funds Rate and Federal Funds Target Rate.
If there's any new data, load to database and then report it via Discord Webhook
"""
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from db.model import engine, fed_funds_eff_rate_tbl
from alert.discord import webhook_finance_monkey
from jobs.fed_funds_target_rate import etl as target_etl
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
load_dotenv()


def etl():
    url = 'https://www.federalreserve.gov/feeds/Data/H15_H15_RIFSPFF_N.B.XML'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "xml")

    data = {
        'date': soup.item.statistics.otherStatistic.observationPeriod.string,
        'value': soup.item.statistics.otherStatistic.value.string,
        'id': str(soup.item.statistics.otherStatistic.observationPeriod.string).replace('-', ''),
    }

    with engine.connect() as conn:
        insert_stmt = insert(fed_funds_eff_rate_tbl).values(data)
        insert_stmt = insert_stmt.on_conflict_do_nothing()
        conn.execute(insert_stmt)
        conn.commit()
        rates_df = pd.read_sql(f"""
        SELECT A.date,
               A.value,
               B.value_upper,
               B.value_lower,
               (B.value_upper + B.value_lower)/2 AS target_avg
        FROM federal_funds_effective_rate A
        LEFT JOIN public.federal_funds_target_rate B on A.id = B.id
        WHERE A.date IN (
            current_date,
            current_date - 1,
            current_date - 2,
            current_date - 3,
            current_date - 4,
            current_date - 5,
            date_trunc('month', current_date) - interval '1 month',
            date_trunc('month', current_date) - interval '2 month',
            date_trunc('month', current_date) - interval '3 month',
            date_trunc('month', current_date) - interval '4 month',
            date_trunc('month', current_date) - interval '5 month'
        );
        """, conn)

    webhook_finance_monkey(f'New Federal Funds Effective Rate is out.\n'
                           f'Federal funds rate:\n\n\n'
                           + rates_df.to_string(index=False, justify='match-parent'))


if __name__ == '__main__':
    target_etl()
    etl()
