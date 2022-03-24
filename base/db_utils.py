import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert

import geopandas as gpd
import pandas as pd

from base.db import engine


# For upsert: https://stackoverflow.com/questions/55187884/insert-into-postgresql-table-from-pandas-with-on-conflict-update
meta = sqlalchemy.MetaData()
meta.bind = engine
meta.reflect(views=True)


def get_upsert_method(constraint_name):
    def upsert(table, conn, keys, data_iter):
        upsert_args = {"constraint": constraint_name}
        for data in data_iter:
            data = {k: data[i] for i, k in enumerate(keys)}
            upsert_args["set_"] = data
            insert_stmt = insert(meta.tables[table.name]).values(**data)
            upsert_stmt = insert_stmt.on_conflict_do_update(**upsert_args)
            conn.execute(upsert_stmt)

    return upsert


def upsert(df, table, constraint_name):
    if isinstance(df, gpd.GeoDataFrame):
        df.to_postgis(table,
                      con=engine,
                      if_exists="append",
                      index=False,
                      method=get_upsert_method(constraint_name))

    elif isinstance(df, pd.DataFrame):
        df.to_sql(table,
                  con=engine,
                  if_exists="append",
                  index=False,
                  method=get_upsert_method(constraint_name))
