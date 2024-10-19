from sqlalchemy import Table, Integer, Double, create_engine, MetaData, Column, Date, func, DateTime


db_str = f'postgresql+psycopg2://postgres:postgres@localhost:5432/postgres'
engine = create_engine(db_str)

meta = MetaData()

fed_funds_eff_rate_tbl = Table('federal_funds_effective_rate',
                               meta,
                               Column('id', Integer, primary_key=True),
                               Column('date', Date),
                               Column('value', Double),
                               Column('created_date', DateTime,
                                      server_default=func.now(),
                                      server_onupdate=func.now())
                               )

meta.create_all(engine)