import pandas as pd
from base.db_utils import upsert
from base.utils import to_list
from base.models import DB_TABLE_COMMODITY
from base.models import Commodity
from base import COMMODITY_GROUPING_DEFAULT
from base.db import session
from sqlalchemy.dialects.postgresql import JSONB


def fill():
    """
    Fill terminals from MaritimeTraffic and manually labelled data
    :return:
    """
    commodities_df = pd.read_csv("assets/commodities.csv")
    commodities_df["alternative_groups"] = commodities_df.alternative_groups.apply(eval)
    upsert(
        df=commodities_df,
        table=DB_TABLE_COMMODITY,
        constraint_name="commodity_pkey",
        dtype={"alternative_groups": JSONB},
    )
    return


def get_ids(transport=None):
    query = session.query(Commodity.id)
    if transport:
        query = query.filter(Commodity.transport.in_(to_list(transport)))

    return [x[0] for x in query.all()]


def get_subquery(session, grouping_name=None):
    """
    Returns a Commodity model for sql alchemy,
    using either default grouping or the specified alternative one
    :param alternative_grouping:
    :return:
    """
    if not grouping_name or grouping_name == COMMODITY_GROUPING_DEFAULT:
        return session.query(
            Commodity.id,
            Commodity.transport,
            Commodity.name,
            Commodity.pricing_commodity,
            Commodity.group,
            Commodity.group_name,
        ).subquery()
    else:
        return session.query(
            Commodity.id,
            Commodity.transport,
            Commodity.name,
            Commodity.pricing_commodity,
            Commodity.alternative_groups[grouping_name].label("group"),
            Commodity.alternative_groups[grouping_name].label("group_name"),
        ).subquery()
