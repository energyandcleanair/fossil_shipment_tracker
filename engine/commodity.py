import pandas as pd
from base.db_utils import upsert
from base.models import DB_TABLE_COMMODITY
from base.models import Commodity
from base import COMMODITY_GROUPING_DEFAULT
from sqlalchemy.dialects.postgresql import JSONB


def fill():
    """
    Fill terminals from MaritimeTraffic and manually labelled data
    :return:
    """
    commodities_df = pd.read_csv("assets/commodities.csv")
    commodities_df["alternative_groups"] = commodities_df.alternative_groups.apply(eval)
    upsert(df=commodities_df,
           table=DB_TABLE_COMMODITY,
           constraint_name='commodity_pkey',
           dtype={'alternative_groups': JSONB})
    return


def get_subquery(session, grouping_name=None):
    """
    Returns a Commodity model for sql alchemy,
    using either default grouping or the specified alternative one
    :param alternative_grouping:
    :return:
    """
    if not grouping_name or grouping_name==COMMODITY_GROUPING_DEFAULT:
        return session.query(Commodity).subquery()
    else:
        return session.query(Commodity.id,
                             Commodity.transport,
                             Commodity.name,
                             Commodity.pricing_commodity,
                             Commodity.alternative_groups[grouping_name].label('group'),
                             Commodity.alternative_groups[grouping_name].label('group_name')
                             ).subquery()