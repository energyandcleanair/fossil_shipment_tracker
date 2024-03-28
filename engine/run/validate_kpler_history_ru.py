from engines import (
    portcall,
    departure,
    arrival,
    shipment,
    position,
    ship,
    counter,
    alert,
    backuper,
    kpler_scraper,
    kpler_trade_computed,
)

from engines.kpler_scraper.verify import KplerTradeVerifier
import integrity
import base
from base.logger import logger_slack

import datetime as dt


def update():
    verifier = KplerTradeVerifier()
    verifier.verify_sync_against_flows(
        origin_iso2s=["RU"],
        platforms=["liquids", "dry", "lng"],
        date_from=dt.date(2021, 1, 1),
        date_to=dt.date.today(),
    )

    integrity.check()
    return


if __name__ == "__main__":
    logger_slack.info(
        "=== Verify historic entries: using %s environment ===" % (base.db.environment,)
    )
    try:
        update()
        logger_slack.info("=== Verify historic entries complete ===")
    except BaseException as e:
        logger_slack.error("=== Verify historic entries failed", stack_info=True, exc_info=True)
        raise e
