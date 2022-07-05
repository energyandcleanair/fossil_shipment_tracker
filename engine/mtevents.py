import pandas as pd
import numpy as np
import sqlalchemy
from tqdm import tqdm

import base
from base.logger import logger, logger_slack
from base.db import session, engine
from base.db_utils import upsert
from base.models import DB_TABLE_MTEVENT_TYPE
from base.utils import to_datetime, to_list

from base.models import MarineTrafficEventType


def create_mtevent_table(force_rebuild=False):
    """
    This function creates the mtevent_type table which stores information
    about different event types, ids and descriptions

    Parameters
    ----------
    force_rebuild : if table already exists, delete data and refill

    Returns
    -------

    """

    # create table if it doesn't exist already
    if not sqlalchemy.inspect(engine).has_table(DB_TABLE_MTEVENT_TYPE):
        logger.info("Table {} doesn't exist yet, we will attempt to create.".format(DB_TABLE_MTEVENT_TYPE))
        MarineTrafficEventType.__table__.create(engine)

    mtevent_type_df = pd.read_csv('assets/mtevent_type.csv')

    assert mtevent_type_df['id'].notnull().values.all()

    mtevent_type_df['id'] = mtevent_type_df['id'].apply(str)
    mtevent_type_df['name'] = mtevent_type_df['name'].apply(str)
    mtevent_type_df['description'] = mtevent_type_df['description'].apply(str)
    mtevent_type_df['availability'] = mtevent_type_df['availability'].apply(str)

    # cna have empty descriptions
    mtevent_type_df.replace({np.nan: None}, inplace=True)

    if force_rebuild:
        try:
            session.query(MarineTrafficEventType).delete()
            session.commit()
        except:
            session.rollback()

    # upsert event types
    upsert(df=mtevent_type_df, table=DB_TABLE_MTEVENT_TYPE, constraint_name="unique_event_type_id")
    session.commit()
    return