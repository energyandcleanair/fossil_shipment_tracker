import logging
import warnings
from base.logger import logger, logger_slack, notify_engineers
from base.db import session
from base.models.kpler import KplerSyncHistory
from base.utils import to_datetime
import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from engines import kpler_scraper

from .update_zones import update_zones
from .update_trade import update_trades
from .verify import KplerTradeVerifier
from .clean_outdated_entries import clean_outdated_entries

from enum import Enum

import datetime as dt
from sqlalchemy import func


class UpdateStatus(Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class UpdateParts(Enum):
    UPDATE_ZONES = "ZONES"
    UPDATE_RECENT_TRADES = "TRADES"
    REFETCH_OUTDATED_HISTORIC_ENTRIES = "REFETCH_OUTDATED_HISTORIC_ENTRIES"


def update_full():
    return update(
        recent_date_from=-30,
        origin_iso2s=["RU", "TR", "CN", "MY", "EG", "AE", "SA", "IN", "SG", "QA", "US", "DZ", "NO"],
    )


def update_lite(
    date_from=-30,
    origin_iso2s=["RU"],
    platforms=kpler_scraper.PLATFORMS,
):
    return update(
        recent_date_from=date_from,
        origin_iso2s=origin_iso2s,
        platforms=platforms,
    )


def update(
    recent_date_from=-30,
    recent_date_to=None,
    historic_date_from="2021-01-01",
    historic_date_to=-30,
    platforms=kpler_scraper.PLATFORMS,
    origin_iso2s=["RU", "TR", "CN", "MY", "EG", "AE", "SA", "IN", "SG"],
    parts=[
        UpdateParts.UPDATE_ZONES,
        UpdateParts.UPDATE_RECENT_TRADES,
        UpdateParts.REFETCH_OUTDATED_HISTORIC_ENTRIES,
    ],
):
    logger_slack.info("=== Updating Kpler ===")
    try:

        verifier = KplerTradeVerifier()

        if UpdateParts.UPDATE_ZONES in parts:
            update_zones()

        if UpdateParts.UPDATE_RECENT_TRADES in parts:
            logger.info("Updating trades")
            update_trades(
                date_from=recent_date_from,
                date_to=recent_date_to,
                platforms=platforms,
                origin_iso2s=origin_iso2s,
            )
            logger.info("Cleaning outdated entries")
            clean_outdated_entries()
            logger.info("Verifying recent against live flows")
            verifier.verify_sync_against_flows(
                origin_iso2s=origin_iso2s,
                platforms=platforms,
                date_from=recent_date_to,
                date_to=recent_date_to,
            )

        if UpdateParts.REFETCH_OUTDATED_HISTORIC_ENTRIES in parts:
            logger.info("Fix invalid historic entries")

            logger.info("Checking for invalid historic entries")
            verifier.verify_sync_against_flows(
                origin_iso2s=origin_iso2s,
                platforms=platforms,
                date_from=historic_date_from,
                date_to=historic_date_to,
            )
            update_outdated_historic_trades(
                origin_iso2s=origin_iso2s,
                platforms=platforms,
                date_from=historic_date_from,
                date_to=historic_date_to,
            )
            logger.info("Cleaning outdated entries")
            clean_outdated_entries()
            logger.info("Verifying historic against live flows")
            verifier.verify_sync_against_flows(
                origin_iso2s=origin_iso2s,
                platforms=platforms,
                date_from=historic_date_from,
                date_to=historic_date_to,
            )

        return UpdateStatus.SUCCESS

    except Exception as e:
        logger_slack.error(
            f"Kpler update failed",
            stack_info=True,
            exc_info=True,
        )
        notify_engineers("Please check error")
        return UpdateStatus.FAILED


def update_outdated_historic_trades(
    origin_iso2s,
    platforms,
    date_from,
    date_to,
):

    date_from = to_datetime(date_from).date() if date_from is not None else dt.date(2021, 1, 1)
    date_to = (
        to_datetime(date_to).date()
        if date_to is not None
        else dt.date.today() - dt.timedelta(days=1)
    )

    # Get sync history for origin iso2s and platforms for the given date range

    month_column = func.date_trunc("month", KplerSyncHistory.date).label("month")
    entries_count = func.count(KplerSyncHistory.id)

    query = (
        session.query(
            KplerSyncHistory.country_iso2,
            KplerSyncHistory.platform,
            month_column,
            entries_count,
        )
        .filter(
            KplerSyncHistory.country_iso2.in_(origin_iso2s),
            KplerSyncHistory.platform.in_(platforms),
            KplerSyncHistory.date >= date_from,
            KplerSyncHistory.date <= date_to,
            KplerSyncHistory.is_valid == False,
        )
        .group_by(
            KplerSyncHistory.country_iso2,
            KplerSyncHistory.platform,
            month_column,
        )
        .having(
            entries_count > 0,
        )
        .order_by(
            KplerSyncHistory.country_iso2,
            KplerSyncHistory.platform,
            month_column,
        )
    )

    # Read into a dataframe
    months_with_missing_data = pd.read_sql(query.statement, session.bind)

    update_time = dt.datetime.now()

    with logging_redirect_tqdm(loggers=[logging.root]), warnings.catch_warnings():
        for _, row in tqdm(months_with_missing_data.iterrows(), unit="missing-month"):
            logger.info(
                f"Updating invalid entries for {row['country_iso2']} on {row['platform']} for {row['month']}"
            )

            month_start = row["month"]
            month_end = month_start + pd.offsets.MonthEnd(0)

            update_trades(
                date_from=month_start,
                date_to=month_end,
                platforms=[row["platform"]],
                origin_iso2s=[row["country_iso2"]],
                update_time=update_time,
            )
