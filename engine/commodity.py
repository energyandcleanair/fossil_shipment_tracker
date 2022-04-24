import pandas as pd
from base.db_utils import upsert
from base.models import DB_TABLE_COMMODITY


def fill():
    """
    Fill terminals from MaritimeTraffic and manually labelled data
    :return:
    """
    commodities_df = pd.read_csv("assets/commodities.csv")
    upsert(df=commodities_df,
           table=DB_TABLE_COMMODITY,
           constraint_name='commodity_pkey')
    return

