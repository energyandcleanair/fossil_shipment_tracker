import datetime as dt
import json
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


from base.models import (
    DB_TABLE_KPLER_PRODUCT,
    DB_TABLE_KPLER_FLOW,
    DB_TABLE_KPLER_TRADE,
    DB_TABLE_KPLER_ZONE,
    DB_TABLE_KPLER_VESSEL,
)
from base.db_utils import upsert
from base.db import session, engine
from base.logger import logger


def upload_trades(trades, ignore_if_copy_failed=False):

    # Ensure this is a pandas dataframe
    if not isinstance(trades, pd.DataFrame):
        trades = pd.DataFrame(trades)

    if trades is not None:
        try:
            # trades["others"] = trades.others.apply(json.dumps)
            trades = trades[~pd.isnull(trades.product_id)]
            trades.to_sql(
                DB_TABLE_KPLER_TRADE,
                con=engine,
                if_exists="append",
                index=False,
            )
        except sa.exc.IntegrityError:
            if ignore_if_copy_failed:
                logger.info("Some rows already exist. Skipping")
            else:
                logger.info("Some rows already exist. Upserting instead")
                upsert(trades, DB_TABLE_KPLER_TRADE, DB_TABLE_KPLER_TRADE + "_pkey")


def upload_flows(flows, ignore_if_copy_failed=False):

    # Ensure this is a pandas dataframe
    if not isinstance(flows, pd.DataFrame):
        flows = pd.DataFrame(flows)

    if len(flows) == 0:
        return None

    pd.DataFrame(flows)["updated_on"] = dt.datetime.now()

    if "destination_country" in flows.columns:
        flows.drop(columns=["destination_country"], inplace=True)

    if len(flows) > 0:
        try:
            flows.to_sql(
                DB_TABLE_KPLER_FLOW,
                con=engine,
                if_exists="append",
                index=False,
            )
        except sa.exc.IntegrityError:
            if ignore_if_copy_failed:
                logger.info("Some rows already exist. Skipping")
            else:
                logger.info("Some rows already exist. Upserting instead")
                upsert(flows, DB_TABLE_KPLER_FLOW, "unique_kpler_flow")


def upload_products(products, ignore_if_copy_failed=False):

    # Ensure this is a pandas dataframe
    if not isinstance(products, pd.DataFrame):
        products = pd.DataFrame(products)

    products = products.drop_duplicates(subset=["id"])
    if len(products) == 0:
        return None

    if len(products) > 0:
        try:
            products.to_sql(
                DB_TABLE_KPLER_PRODUCT,
                con=engine,
                if_exists="append",
                index=False,
            )
        except sa.exc.IntegrityError:
            if ignore_if_copy_failed:
                logger.info("Some rows already exist. Skipping")
            else:
                logger.info("Some rows already exist. Upserting instead")
                upsert(products, DB_TABLE_KPLER_PRODUCT, DB_TABLE_KPLER_PRODUCT + "_pkey")


def upload_zones(zones, ignore_if_copy_failed=False):

    # Ensure this is a pandas dataframe
    if not isinstance(zones, pd.DataFrame):
        zones = pd.DataFrame(zones)

    if len(zones) == 0:
        return None

    zones = zones.drop_duplicates(subset=["id"])
    # Drop zones with id = None
    zones = zones[~pd.isnull(zones.id)]
    if len(zones) > 0:
        try:
            zones.to_sql(
                DB_TABLE_KPLER_ZONE,
                con=engine,
                if_exists="append",
                index=False,
            )
        except sa.exc.IntegrityError:
            if ignore_if_copy_failed:
                logger.info("Some rows already exist. Skipping")
            else:
                logger.info("Some rows already exist. Upserting instead")
                upsert(zones, DB_TABLE_KPLER_ZONE, DB_TABLE_KPLER_ZONE + "_pkey")


def upload_vessels(vessels, ignore_if_copy_failed=False):

    # Ensure this is a pandas dataframe
    if not isinstance(vessels, pd.DataFrame):
        vessels = pd.DataFrame(vessels)

    if len(vessels) == 0:
        return None

    vessels = vessels.drop_duplicates(subset=["id"])

    if len(vessels) > 0:
        try:
            vessels.to_sql(
                DB_TABLE_KPLER_VESSEL,
                con=engine,
                if_exists="append",
                index=False,
                dtype={"others": JSONB},
            )
        except sa.exc.IntegrityError:
            if ignore_if_copy_failed:
                logger.info("Some rows already exist. Skipping")
            else:
                logger.info("Some rows already exist. Upserting instead")
                upsert(vessels, DB_TABLE_KPLER_VESSEL, DB_TABLE_KPLER_VESSEL + "_pkey")