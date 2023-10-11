import os
import datetime
import pandas as pd
from sqlalchemy import create_engine

old_print = print
def timestamped_print(*args, **kwargs):
  old_print(datetime.datetime.now(), *args, **kwargs)
print = timestamped_print

def postgres_upsert(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)
    
    
def postgres_safe_insert(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_nothing(
        constraint=f"{table.table.name}_pkey"
    )
    conn.execute(upsert_statement)

def main():
    print("Entering main of seed_api_data.py")
    
    ########################
    # Establish DB engine  #
    ########################

    SQLALCHEMY_DATABASE_URL = "postgresql://" + os.environ.get('POSTGRESQL_USER') + ":" + os.environ.get(
        'POSTGRESQL_PASSWORD') + "@" + os.environ.get('POSTGRESQL_HOSTNAME') + "/" + os.environ.get('POSTGRESQL_DATABASE')

    engine = create_engine(SQLALCHEMY_DATABASE_URL)

    end_date = pd.to_datetime(datetime.datetime.strptime(os.environ.get('END_DATE'), "%Y-%m-%d %H:%M:%S"))
    start_date = end_date - datetime.timedelta(days=60)

    query = "select * from external_api_data WHERE date >= '{start_date}' AND date <= '{end_date}'"
    data = pd.read_sql_query(query, engine).sort_values(['date']).drop_duplicates(subset=['date'])
    data = data.round({'value': 3})
    data.set_index(['id', 'date', 'api_name', 'type'], inplace=True)
    data.to_sql("api_data", engine, if_exists = "append", method=postgres_upsert)
    print("DATA COPIED")

    return
    query = "select date, atm_pressure as value from sensor_water_depth where place='Carolina Beach, North Carolina' and date < '2022-12-17 17:05:00'"
    # query = "select date, sensor_pressure as value from sensor_water_depth where atm_data_src='FIMAN' and atm_station_id='30046' and date < '2022-12-01 17:05:00'"
    data = pd.read_sql_query(query, engine).sort_values(['date']).drop_duplicates(subset=['date'])
    data['id'] = 30046
    data['api_name'] = 'FIMAN'
    data['type'] = 'pressure'
    data.set_index(['id', 'date', 'api_name', 'type'], inplace=True)
    data.to_sql("api_data", engine, if_exists = "append", method=postgres_upsert)
    print("DATA INSERTED")

    engine.dispose()

if __name__ == "__main__":
    main()