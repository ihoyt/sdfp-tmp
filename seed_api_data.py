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

    # end_date = pd.to_datetime(datetime.datetime.strptime(os.environ.get('END_DATE'), "%Y-%m-%d %H:%M:%S"))
    # start_date = end_date - datetime.timedelta(days=30)

    # end_date = pd.read_sql_query("SELECT min(date) as date FROM api_data", engine)
    # if end_date.iloc[0]["date"] is None:
    #     print("No old data to be processed")
    #     return
    
    # start_date = end_date.at[0, 'date'] - datetime.timedelta(days=60)
    # end_date = end_date.at[0, 'date']

    # print(start_date)
    # print(end_date)
    # query = f"select * from external_api_data WHERE date >= '{start_date}' AND date <= '{end_date}'"
    # data = pd.read_sql_query(query, engine).sort_values(['date']).drop_duplicates(subset=['date'])
    # data = data.round({'value': 3})
    # data.set_index(['id', 'date', 'api_name', 'type'], inplace=True)
    # data.to_sql("api_data", engine, if_exists = "append", method=postgres_upsert)
    # print("DATA COPIED")

    # return


    end_date = pd.read_sql_query("SELECT min(date) as date FROM api_data", engine)
    if end_date.iloc[0]["date"] is None:
        print("No old data to be processed")
        return
    
    start_date = end_date.at[0, 'date'] - datetime.timedelta(days=60)
    end_date = end_date.at[0, 'date']
    print(start_date)
    print(end_date)
    query = f"select date, atm_pressure as value from sensor_water_depth where place='Carolina Beach, North Carolina' and date >= '{start_date}' and date < '{end_date}'"
    # query = "select date, atm_pressure as value from sensor_water_depth where place='Carolina Beach, North Carolina' and date < '2022-12-17 17:05:00'"
    data = pd.read_sql_query(query, engine).sort_values(['date']).drop_duplicates(subset=['date'])
    # print(data)
    # data.set_index(['date']);
    data = data.resample('3T', on='date').mean().dropna()
    # data = data.resample('3T').mean().dropna()
    data.reset_index(inplace=True)
    # print(data)
    data['id'] = 30046
    data['api_name'] = 'FIMAN'
    data['type'] = 'pressure'
    data.set_index(['id', 'date', 'api_name', 'type'], inplace=True)
    data.to_sql("api_data", engine, if_exists = "append", method=postgres_upsert)
    print("DATA INSERTED")

    engine.dispose()

if __name__ == "__main__":
    main()