import datetime as dt
import json
import os
import pandas as pd
from tqdm import tqdm
import sqlalchemy as sa

from base.utils import to_datetime, to_list
from base import UNKNOWN_COUNTRY
from base.models import (
    DB_TABLE_KPLER_PRODUCT,
    DB_TABLE_KPLER_FLOW,
    DB_TABLE_KPLER_TRADE,
)
from base.db_utils import upsert
from base.db import session, engine
from base.logger import logger_slack, slacker, notify_engineers

from base.logger import logger

from kpler.sdk import FlowsDirection, FlowsSplit, FlowsPeriod, FlowsMeasurementUnit

from .scraper import KplerScraper
from .scraper_flow import KplerFlowScraper
from .scraper_trade import KplerTradeScraper
from .scraper_product import KplerProductScraper
from .upload import upload_flows, upload_trades

from .update_trade import update_trades
from .update_flow import update_flows


def update_full():
    return update(
        date_from=-30,
        origin_iso2s=["RU", "TR", "CN", "MY", "EG", "AE", "SA", "IN", "SG", "QA", "US", "DZ", "NO"],
        from_splits=[FlowsSplit.OriginCountries, FlowsSplit.OriginPorts],
        to_splits=[FlowsSplit.DestinationCountries, FlowsSplit.DestinationPorts],
    )


def update_lite(
    date_from=-30,
    origin_iso2s=["RU"],
    from_splits=[FlowsSplit.OriginCountries],
    to_splits=[FlowsSplit.DestinationCountries],
    platforms=None,
):
    return update(
        date_from=date_from,
        origin_iso2s=origin_iso2s,
        from_splits=from_splits,
        to_splits=to_splits,
        platforms=platforms,
    )


def update(
    date_from=-30,
    date_to=None,
    platforms=None,
    products=None,
    origin_iso2s=["RU", "TR", "CN", "MY", "EG", "AE", "SA", "IN", "SG"],
    from_splits=[FlowsSplit.OriginCountries, FlowsSplit.OriginPorts],
    to_splits=[FlowsSplit.DestinationCountries, FlowsSplit.DestinationPorts],
    ignore_if_copy_failed=False,
    use_brute_force=True,
    add_unknown=True,
    add_unknown_only=False,
):

    try:
        update_flows(
            date_from=date_from,
            date_to=date_to,
            platforms=platforms,
            origin_iso2s=origin_iso2s,
            from_splits=from_splits,
            to_splits=to_splits,
            ignore_if_copy_failed=ignore_if_copy_failed,
            use_brute_force=use_brute_force,
            add_unknown=add_unknown,
            add_unknown_only=add_unknown_only,
        )

        update_trades(
            date_from=date_from,
            date_to=date_to,
            platforms=platforms,
            origin_iso2s=origin_iso2s,
            ignore_if_copy_failed=ignore_if_copy_failed,
        )

        update_is_valid()

    except Exception as e:
        logger_slack.error("Kpler update failed: %s" % (str(e),))
        notify_engineers("Please check error")


def update_is_valid():
    # Read sql from 'update_is_valid.sql'
    with open(os.path.join(os.path.dirname(__file__), "update_is_valid.sql")) as f:
        sql = f.read()
    session.execute(sql)
    session.commit()
    return
