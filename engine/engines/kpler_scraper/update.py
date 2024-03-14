from base.logger import logger, logger_slack, notify_engineers

from engines import kpler_scraper

from .update_zones import update_zones
from .update_trade import update_trades
from .verify import KplerTradeVerifier
from .clean_outdated_entries import clean_outdated_entries

from enum import Enum


class UpdateStatus(Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class UpdateParts(Enum):
    ZONES = "ZONES"
    TRADES = "TRADES"
    CLEAN_OUTDATED_ENTRIES = "CLEAN_OUTDATED_ENTRIES"
    VERIFY_AGAINST_LIVE_FLOWS = "VERIFY_AGAINST_LIVE_FLOWS"


def update_full():
    return update(
        date_from=-30,
        origin_iso2s=["RU", "TR", "CN", "MY", "EG", "AE", "SA", "IN", "SG", "QA", "US", "DZ", "NO"],
    )


def update_lite(
    date_from=-30,
    origin_iso2s=["RU"],
    platforms=kpler_scraper.PLATFORMS,
):
    return update(
        date_from=date_from,
        origin_iso2s=origin_iso2s,
        platforms=platforms,
    )


def update(
    date_from=-30,
    date_to=None,
    platforms=kpler_scraper.PLATFORMS,
    origin_iso2s=["RU", "TR", "CN", "MY", "EG", "AE", "SA", "IN", "SG"],
    parts=[
        UpdateParts.ZONES,
        UpdateParts.TRADES,
        UpdateParts.CLEAN_OUTDATED_ENTRIES,
        UpdateParts.VERIFY_AGAINST_LIVE_FLOWS,
    ],
):
    logger_slack.info("=== Updating Kpler ===")
    try:
        if UpdateParts.ZONES in parts:
            update_zones()

        if UpdateParts.TRADES in parts:
            logger.info("Updating trades")
            update_trades(
                date_from=date_from,
                date_to=date_to,
                platforms=platforms,
                origin_iso2s=origin_iso2s,
            )

        if UpdateParts.CLEAN_OUTDATED_ENTRIES in parts:
            logger.info("Cleaning outdated entries")
            clean_outdated_entries()

        if UpdateParts.VERIFY_AGAINST_LIVE_FLOWS in parts:
            logger.info("Verifying against live flows")

            verifier = KplerTradeVerifier()

            verifier.verify_sync_against_flows(
                origin_iso2s=origin_iso2s, platforms=platforms, date_from=date_from, date_to=date_to
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
