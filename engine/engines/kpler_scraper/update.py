from base.db import session
from base.logger import logger, logger_slack, notify_engineers

from kpler.sdk import FlowsSplit

from .update_trade import update_trades
from .update_flow import update_flows

from enum import Enum

import os


class UpdateStatus(Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class UpdateParts(Enum):
    FLOWS = "FLOWS"
    TRADES = "TRADES"
    VALIDATE = "VALIDATE"


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
    parts=[UpdateParts.TRADES, UpdateParts.VALIDATE],
):
    return update(
        date_from=date_from,
        origin_iso2s=origin_iso2s,
        from_splits=from_splits,
        to_splits=to_splits,
        platforms=platforms,
        parts=parts,
    )


def update(
    date_from=-30,
    date_to=None,
    platforms=None,
    products=None,
    origin_iso2s=["RU", "TR", "CN", "MY", "EG", "AE", "SA", "IN", "SG"],
    from_splits=[FlowsSplit.OriginCountries, FlowsSplit.OriginPorts],
    to_splits=[FlowsSplit.DestinationCountries, FlowsSplit.DestinationPorts],
    add_unknown=True,
    add_unknown_only=False,
    parts=[UpdateParts.TRADES, UpdateParts.VALIDATE],
):
    logger_slack.info("=== Updating Kpler ===")
    try:
        if UpdateParts.FLOWS in parts:
            logger.info("Updating flows")
            update_flows(
                date_from=date_from,
                date_to=date_to,
                platforms=platforms,
                origin_iso2s=origin_iso2s,
                from_splits=from_splits,
                to_splits=to_splits,
                add_unknown=add_unknown,
                add_unknown_only=add_unknown_only,
            )

        if UpdateParts.TRADES in parts:
            logger.info("Updating trades")
            update_trades(
                date_from=date_from,
                date_to=date_to,
                platforms=platforms,
                origin_iso2s=origin_iso2s,
            )

        if UpdateParts.VALIDATE in parts:
            logger.info("Validating update")
            update_is_valid()

        return UpdateStatus.SUCCESS

    except Exception as e:
        logger_slack.error(
            f"Kpler update failed",
            stack_info=True,
            exc_info=True,
        )
        notify_engineers("Please check error")
        return UpdateStatus.FAILED


def update_is_valid():
    # Read sql from 'update_is_valid.sql'
    with open(os.path.join(os.path.dirname(__file__), "update_is_valid.sql")) as f:
        sql = f.read()
    session.execute(sql)
    session.commit()
    return
