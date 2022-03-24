from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

from base.logger import logger
from base.env import get_env


environment = get_env('ENVIRONMENT', 'development').lower() # development, production, test
connections = {
    'test': get_env('DB_URL_TEST', default=None),
    'development': get_env('DB_URL_DEVELOPMENT', default=None),
    'production': get_env('DB_URL_PRODUCTION', default=None)
}
connection = connections.get(environment)
if connection is None:
    logger.warning("Database connection string not specified")


engine = create_engine(connection,
                       convert_unicode=True,
                       pool_size=5,
                       max_overflow=2,
                       pool_pre_ping=True,
                       pool_timeout=30,
                       pool_recycle=1800
                       )

session = scoped_session(sessionmaker(autocommit=False,
                                      autoflush=False,
                                      bind=engine))

Base = declarative_base()
Base.query = session.query_property()


def init_db(drop_first=False):
    import models
    if drop_first:
        Base.metadata.drop_all(bind=engine)

    Base.metadata.create_all(bind=engine)
