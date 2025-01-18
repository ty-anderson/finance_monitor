"""
Report the fed funds data
"""
import pandas as pd
from db.model import engine
from alert.discord import send_alert
from decorators import try_alert


@try_alert
def report_fed_funds_rate():
    with engine.connect() as conn:
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
            date_trunc('month', current_date) - interval '5 month',
            date_trunc('month', current_date) - interval '6 month',
            date_trunc('month', current_date) - interval '7 month',
            date_trunc('month', current_date) - interval '8 month',
            date_trunc('month', current_date) - interval '9 month',
            date_trunc('month', current_date) - interval '10 month',
            date_trunc('month', current_date) - interval '11 month',
            date_trunc('month', current_date) - interval '12 month',
            date_trunc('month', current_date) - interval '24 month',
            date_trunc('month', current_date) - interval '36 month',
            date_trunc('month', current_date) - interval '48 month',
            date_trunc('month', current_date) - interval '60 month'
        );
                """, conn)

    send_alert(msg=f'New Federal Funds Effective Rate is out.\n'
                   f'Federal funds rate:\n\n\n'
                   + rates_df.to_string(index=False, justify='match-parent'),
               channel='Finance Monkey')


if __name__ == '__main__':
    report_fed_funds_rate()
