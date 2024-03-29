import sys
from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

from base.logger import logger
from base.env import get_env

import numpy as np
from psycopg2.extensions import register_adapter, AsIs

register_adapter(np.int64, AsIs)


environment = get_env("ENVIRONMENT", "test").lower()  # development, production, test


def get_connection_string_from_env():
    envs_to_connection_keys = {
        "test": "DB_URL_TEST",
        "development": "DB_URL_DEVELOPMENT",
        "production": "DB_URL_PRODUCTION",
    }

    connection_env_key = envs_to_connection_keys.get(environment)
    logger.info(f"Getting connection string for {environment}")
    connection = get_env(connection_env_key, default=None)
    if connection is None:
        logger.fatal(f"Database connection string not specified for {environment}")
        raise RuntimeError(f"Database connection string not specified for {environment}")
    return connection


connection = get_connection_string_from_env()

engine = None
try:
    engine = create_engine(
        connection,
        convert_unicode=True,
        pool_size=5,
        max_overflow=2,
        pool_pre_ping=True,
        pool_timeout=30,
        pool_recycle=1800,
    )
except Exception as e:
    logger.error("Could not connect to database.")

session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

Base = declarative_base()
Base.query = session.query_property()


def check_if_table_exists(table, create_table=False):
    """
    Function checks whether table exists in our db and creates the table if
    desired

    Parameters
    ----------
    table : base definition of the table
    create_table : whether to create the table if it does not exist

    Returns
    -------

    """
    table_exists = sqlalchemy.inspect(engine).has_table(table.__tablename__)

    if not table_exists and create_table:
        table.__table__.create(engine)
        # check whether creation was successful
        return sqlalchemy.inspect(engine).has_table(table.__tablename__)
    else:
        return table_exists


def init_db(drop_first=False):
    if drop_first:
        if environment == "test":
            Base.metadata.drop_all(bind=engine)
        else:
            raise ValueError("Are you sure you want to delete db?")

    Base.metadata.create_all(bind=engine)


# For upsert: https://stackovershipment.com/questions/55187884/insert-into-postgresql-table-from-pandas-with-on-conflict-update
meta = None

# For old counter data
from pymongo import MongoClient

mongo_client = MongoClient(get_env("CREA_MONGODB_URL"))
