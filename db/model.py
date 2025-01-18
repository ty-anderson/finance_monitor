from sqlalchemy import (Table, String, Text, Integer, Double,
                        create_engine, MetaData, Column, Date,
                        func, DateTime)


db_str = f'postgresql+psycopg2://postgres:postgres@homeserver:5432/postgres'
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

fed_funds_target_tbl = Table('federal_funds_target_rate',
                             meta,
                             Column('id', Integer, primary_key=True),
                             Column('date', Date),
                             Column('value_upper', Double),
                             Column('value_lower', Double),
                             Column('created_date', DateTime,
                                    server_default=func.now(),
                                    server_onupdate=func.now())
                             )

discord_channel_tbl = Table('discord_channel',
                            meta,
                            Column('id', Integer, primary_key=True),
                            Column('channel_name', String(255)),
                            Column('url', Text)
                            )

meta.create_all(engine)