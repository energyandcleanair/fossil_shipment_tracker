import pandas as pd

import base
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

    # Add Kpler Products
    from engine.kpler_scraper import KplerScraper
    from engine.kpler_scraper import get_product_id, get_commodity_group

    scraper = KplerScraper()
    kpler_products = scraper.get_products(platform="liquids")
    kpler_products = scraper.get_products(platform="dry")

    # Adding the couple products that correspond to a group or family
    # To note: "group" has a different meaning for Kpler and our db
    groups_to_add = [
        "Crude/Co",
        "Gasoil/Diesel",
        "Kero/Jet",
        "Gasoline/Naphtha",
        "Fuel Oils",
    ]
    for group in groups_to_add:
        new = kpler_products[kpler_products.group == group].head(1).copy()
        new.name = group
        kpler_products = pd.concat([kpler_products, new])

    # Use similar grouping names than original commodities
    def corresponding_commodity_field(x, field="group"):
        if x is None:
            return None
        return commodities_df[commodities_df.id == x][field].values[0]

    for field in ["group_name", "group", "alternative_groups"]:
        kpler_products[field] = kpler_products.apply(
            lambda x: corresponding_commodity_field(get_commodity_group(x), field=field), axis=1
        )

    kpler_products["id"] = kpler_products["name"].apply(get_product_id)
    kpler_products["pricing_commodity"] = kpler_products["id"]

    crude_idx = kpler_products.name.isin(["Crude/Co", "Crude", "Condensate"])
    kpler_products.loc[crude_idx, "pricing_commodity"] = base.CRUDE_OIL

    kpler_products["transport"] = base.SEABORNE
    kpler_products["grouping"] = "default"
    kpler_products = kpler_products[commodities_df.columns]

    upsert(
        df=kpler_products,
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
