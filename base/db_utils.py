import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert

import geopandas as gpd
import pandas as pd

from base.db import engine
from tqdm import tqdm
from tqdm.contrib import tzip
# For upsert: https://stackoverflow.com/questions/55187884/insert-into-postgresql-table-from-pandas-with-on-conflict-update
meta = sqlalchemy.MetaData()
meta.bind = engine
meta.reflect(views=True)


def execute_statement(stmt, print_result=True):
    with engine.connect() as con:
        con = con.execution_options(isolation_level="AUTOCOMMIT")
        con.execute(stmt)
        # if print_result:
        #     for row in rs:
        #         print(row)


def get_upsert_method(constraint_name):
    def upsert(table, conn, keys, data_iter):
        upsert_args = {"constraint": constraint_name}
        data_list = list(data_iter)
        for data in tqdm(data_list):
            data = {k: data[i] for i, k in enumerate(keys)}
            upsert_args["set_"] = data
            insert_stmt = insert(meta.tables[table.name]).values(**data)
            upsert_stmt = insert_stmt.on_conflict_do_update(**upsert_args)
            conn.execute(upsert_stmt)

    return upsert


def upsert(df, table, constraint_name, dtype={}):


    if isinstance(df, gpd.GeoDataFrame):
        #TODO upsert not yet supported. Not sure what's the best way to proceed
        # It will fail if constraint is violated
        # A way would be to first remove db records violating the constraint
        df.to_postgis(table,
                      con=engine,
                      if_exists="append",
                      index=False)

    elif isinstance(df, pd.DataFrame):
        df.to_sql(table,
                  con=engine,
                  if_exists="append",
                  index=False,
                  method=get_upsert_method(constraint_name),
                  dtype=dtype)
