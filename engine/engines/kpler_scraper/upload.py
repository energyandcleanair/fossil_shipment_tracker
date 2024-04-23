import datetime as dt
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from sqlalchemy import func

from ..ship import fill

from base.models import (
    DB_TABLE_KPLER_PRODUCT,
    DB_TABLE_KPLER_FLOW,
    DB_TABLE_KPLER_TRADE,
    DB_TABLE_KPLER_ZONE,
    DB_TABLE_KPLER_VESSEL,
    DB_TABLE_KPLER_INSTALLATION,
)
from base.models.kpler import KplerZone
from base.db_utils import upsert
from base.db import engine, session
from base.logger import logger


def upload_trades(trades, update_time=dt.datetime.utcnow()):
    # Ensure this is a pandas dataframe
    if not isinstance(trades, pd.DataFrame):
        trades = pd.DataFrame(trades)

    if trades.empty:
        return None

    if not "updated_on" in trades.columns:
        trades["updated_on"] = update_time

    trades = trades[~pd.isnull(trades.product_id)]
    upsert(trades, DB_TABLE_KPLER_TRADE, DB_TABLE_KPLER_TRADE + "_pkey")


def upload_flows(flows):
    # Ensure this is a pandas dataframe
    if not isinstance(flows, pd.DataFrame):
        flows = pd.DataFrame(flows)

    if len(flows) == 0:
        return None

    pd.DataFrame(flows)["updated_on"] = dt.datetime.now()

    if "destination_country" in flows.columns:
        flows.drop(columns=["destination_country"], inplace=True)

    if len(flows) > 0:
        upsert(flows, DB_TABLE_KPLER_FLOW, "unique_kpler_flow")


def upload_products(products):
    # Ensure this is a pandas dataframe
    if not isinstance(products, pd.DataFrame):
        products = pd.DataFrame(products)

    products = products.drop_duplicates(subset=["id"])
    if len(products) == 0:
        return None

    if len(products) > 0:
        upsert(products, DB_TABLE_KPLER_PRODUCT, DB_TABLE_KPLER_PRODUCT + "_pkey")


def upload_zones(zones):
    # Ensure this is a pandas dataframe
    if not isinstance(zones, pd.DataFrame):
        zones = pd.DataFrame(zones)

    if len(zones) == 0:
        return None

    zones = zones.drop_duplicates(subset=["id"])
    # Drop zones with id = None
    zones = zones[~pd.isnull(zones.id)]
    if len(zones) > 0:
        upsert(zones, DB_TABLE_KPLER_ZONE, DB_TABLE_KPLER_ZONE + "_pkey")


def update_zone_areas():
    session.query(KplerZone).filter(KplerZone.country_iso2 == "RU").update(
        {
            "area": sa.case(
                [
                    (func.ST_X(KplerZone.geometry) > 100, "Pacific"),
                    (func.ST_Y(KplerZone.geometry) > 62, "Arctic"),
                    (func.ST_Y(KplerZone.geometry) > 50, "Baltic"),
                    (func.ST_X(KplerZone.geometry) > 47, "Caspian Sea"),
                ],
                else_="Black sea",
            )
        },
        synchronize_session=False,
    )

    session.commit()


def upload_installations(installations):
    # Ensure this is a pandas dataframe
    if not isinstance(installations, pd.DataFrame):
        installations = pd.DataFrame(installations)

    if len(installations) == 0:
        return None

    installations = installations.drop_duplicates(subset=["id"])
    # Drop installations with id = None
    installations = installations[~pd.isnull(installations.id)]
    if len(installations) > 0:
        upsert(
            installations,
            DB_TABLE_KPLER_INSTALLATION,
            DB_TABLE_KPLER_INSTALLATION + "_pkey",
        )


def upload_vessels(vessels):
    # Ensure this is a pandas dataframe
    if not isinstance(vessels, pd.DataFrame):
        vessels = pd.DataFrame(vessels)

    if len(vessels) == 0:
        return None

    vessels = vessels.drop_duplicates(subset=["id"])

    unique_vessels = vessels.imo.unique()

    not_none_unique_vessels = filter(lambda x: x != None, unique_vessels)

    fill(not_none_unique_vessels)

    if len(vessels) > 0:
        upsert(vessels, DB_TABLE_KPLER_VESSEL, DB_TABLE_KPLER_VESSEL + "_pkey")
