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
    commodities_df["equivalent_id"] = commodities_df["id"]
    upsert(
        df=commodities_df,
        table=DB_TABLE_COMMODITY,
        constraint_name="commodity_pkey",
        dtype={"alternative_groups": JSONB},
    )

    fill_kpler_commodities(commodities_df=commodities_df)


def fill_kpler_commodities(commodities_df):
    # Add Kpler Products
    from engine.kpler_scraper import KplerScraper, KplerProductScraper
    from engine.kpler_scraper import get_product_id, get_commodity_equivalent, get_commodity_pricing
    from engine.kpler_scraper import upload_products

    kpler_products = KplerProductScraper().get_products_brute()
    from base.models import KplerProduct

    # kpler_products = pd.read_sql(
    #     KplerProduct.query.statement,
    #     session.bind,
    # )
    # commodities_df.columns
    kpler_products = [x for x in kpler_products if x is not None]

    # First upload in kpler_products table
    upload_products(kpler_products)

    # then into commodity table
    kpler_products = pd.DataFrame(kpler_products).rename(
        columns={"group_name": "group", "family_name": "family"}
    )

    kpler_products.drop_duplicates(subset=["id", "platform"], inplace=True)

    def add_groups_as_commodities(kpler_products):
        # Adding the couple products that correspond to a group or family
        # To note: "group" has a different meaning for Kpler and our db
        groups_to_add = [
            "Crude/Co",
            "Gasoil/Diesel",
            "Kero/Jet",
            "Gasoline/Naphtha",
            "Fuel Oils",
            "Coal",
        ]
        for group in groups_to_add:
            new = kpler_products[kpler_products.group == group].head(1).copy()
            new.name = group
            kpler_products = pd.concat([kpler_products, new])

        return kpler_products

    kpler_products = add_groups_as_commodities(kpler_products)
    kpler_products["id"] = kpler_products["name"].apply(get_product_id)
    kpler_products["equivalent_id"] = kpler_products.apply(get_commodity_equivalent, axis=1)
    kpler_products["pricing_commodity"] = kpler_products.apply(get_commodity_pricing, axis=1)

    # Add fields from commodities
    kpler_products = pd.merge(
        kpler_products.drop(columns=["group"]),
        commodities_df[["id", "group_name", "group", "alternative_groups"]].rename(
            columns={"id": "equivalent_id"}
        ),
        how="left",
    )

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
