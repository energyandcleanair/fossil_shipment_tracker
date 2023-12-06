import sqlalchemy
from sqlalchemy.dialects.postgresql import insert
import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from base.db import engine, meta  # KEEP meta, even though it is greyed out by IDE
from base.logger import logger, logger_slack


def execute_statement(stmt, print_result=False, slack_result=False):
    with engine.connect() as con:
        con = con.execution_options(isolation_level="AUTOCOMMIT")
        if print_result or slack_result:
            rs = con.execute(stmt)
            rows = [str(x) for x in rs]
            for row in rows:
                if print_result:
                    print(row)
            if slack_result:
                logger_slack.info("\n".join(rows))
        else:
            con.execute(stmt)


def get_upsert_method(constraint_name, show_progress=True):
    def upsert(table, conn, keys, data_iter):
        upsert_args = {"constraint": constraint_name}
        data_list = list(data_iter)
        global meta
        if show_progress:
            data_iterator = tqdm(data_list, unit=f"rows({table})", leave=False)
        else:
            data_iterator = data_list

        for data in data_iterator:
            data = {k: data[i] for i, k in enumerate(keys)}
            upsert_args["set_"] = data
            insert_stmt = insert(meta.tables[table.name]).values(**data)
            upsert_stmt = insert_stmt.on_conflict_do_update(**upsert_args)
            conn.execute(upsert_stmt)

    return upsert


def upsert(df, table, constraint_name, dtype={}, show_progress=True, chunksize=10000):
    """
    This function upserts data into a specific table using chunks determined by chunksize

    :param df:
    :param table:
    :param constraint_name:
    :param dtype:
    :param show_progress:
    :param chunksize:
    :return:
    """
    global meta
    if meta is None:
        meta = sqlalchemy.MetaData()
        meta.bind = engine
        meta.reflect(views=False, resolve_fks=False)

    if isinstance(df, gpd.GeoDataFrame):
        # TODO upsert not yet supported. Not sure what's the best way to proceed
        # It will fail if constraint is violated
        # A way would be to first remove db records violating the constraint
        df.to_postgis(table, con=engine, if_exists="append", index=False)

    elif isinstance(df, pd.DataFrame):
        df.to_sql(
            table,
            con=engine,
            if_exists="append",
            index=False,
            method=get_upsert_method(constraint_name, show_progress=show_progress),
            chunksize=chunksize,
            dtype=dtype,
        )
