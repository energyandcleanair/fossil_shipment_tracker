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
from engines.kpler_scraper.checks_data_source import update_sync_history_with_status

from .update_zones import update_zones
from .update_trade import update_trades
from .verify import KplerTradeComparer
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
):
    return update(
        recent_date_from=date_from,
        origin_iso2s=origin_iso2s,
    )


def update(
    recent_date_from=-30,
    recent_date_to=None,
    historic_date_from="2021-01-01",
    historic_date_to=-30,
    origin_iso2s=["RU", "TR", "CN", "MY", "EG", "AE", "SA", "IN", "SG"],
    parts=[
        UpdateParts.UPDATE_ZONES,
        UpdateParts.UPDATE_RECENT_TRADES,
        UpdateParts.REFETCH_OUTDATED_HISTORIC_ENTRIES,
    ],
):
    logger_slack.info("=== Updating Kpler ===")
    try:

        if UpdateParts.UPDATE_ZONES in parts:
            update_zones()

        if UpdateParts.UPDATE_RECENT_TRADES in parts:
            logger.info("Updating trades")
            update_trades(
                date_from=recent_date_from,
                date_to=recent_date_to,
                origin_iso2s=origin_iso2s,
            )
            logger.info("Cleaning outdated entries")
            clean_outdated_entries()

            validate_sync(
                origin_iso2s=origin_iso2s,
                date_from=recent_date_from,
                date_to=recent_date_to,
            )

        if UpdateParts.REFETCH_OUTDATED_HISTORIC_ENTRIES in parts:
            logger.info("Fix invalid historic entries")

            update_historic_trades(
                origin_iso2s=origin_iso2s,
                date_from=historic_date_from,
                date_to=historic_date_to,
            )
            logger.info("Cleaning outdated entries")
            clean_outdated_entries()

            validate_sync(
                origin_iso2s=origin_iso2s,
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


def update_historic_trades(
    *,
    origin_iso2s,
    date_from,
    date_to,
):

    date_from = to_datetime(date_from).date() if date_from is not None else dt.date(2021, 1, 1)
    date_to = (
        to_datetime(date_to).date()
        if date_to is not None
        else dt.date.today() - dt.timedelta(days=1)
    )

    # Get sync history for origin iso2s for the given date range

    logger.info("Checking for invalid historic entries")

    comparer = KplerTradeComparer()

    for origin_iso2 in origin_iso2s:

        comparison = comparer.compare_to_live_flows(
            origin_iso2=origin_iso2,
            date_from=date_from,
            date_to=date_to,
        )

        failed_entries = comparison[comparison["problems"] > 0]

        # Convert failed days to a list of failed months
        failed_months = failed_entries["departure_day"].dt.to_period("M").unique()

        update_time = dt.datetime.now()

        with logging_redirect_tqdm(loggers=[logging.root]), warnings.catch_warnings():
            for month in tqdm(failed_months, unit="missing-month"):
                logger.info(f"Updating invalid entries for {origin_iso2} for {month}")

                month_start = month.start_time.date()
                month_end = month.end_time.date()

                update_trades(
                    date_from=month_start,
                    date_to=month_end,
                    origin_iso2s=[origin_iso2],
                    update_time=update_time,
                )


def validate_sync(
    origin_iso2s=None,
    date_from=None,
    date_to=None,
):

    date_from = to_datetime(date_from).date() if date_from is not None else dt.date(2021, 1, 1)
    date_to = (
        to_datetime(date_to).date()
        if date_to is not None
        else dt.date.today() - dt.timedelta(days=1)
    )
    comparer = KplerTradeComparer()
    checked_time = dt.datetime.now()
    for origin_iso2 in origin_iso2s:

        comparison = comparer.compare_to_live_flows(
            origin_iso2=origin_iso2,
            date_from=date_from,
            date_to=date_to,
        )

        update_sync_history_with_status(
            origin_iso2=origin_iso2,
            date_from=date_from,
            date_to=date_to,
            comparison=comparison,
            checked_time=checked_time,
        )
