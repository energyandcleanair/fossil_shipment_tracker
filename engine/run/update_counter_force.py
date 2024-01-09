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

import integrity
import base
from base.logger import logger_slack

import datetime as dt


def update():
    counter.update(version=base.COUNTER_VERSION0, force=True)
    counter.update(version=base.COUNTER_VERSION1, force=True)
    counter.update(version=base.COUNTER_VERSION2, force=True)
    integrity.check()
    return


if __name__ == "__main__":
    logger_slack.info(
        "=== Update counter forced: using %s environment ===" % (base.db.environment,)
    )
    try:
        update()
        logger_slack.info("=== Update counter forced complete ===")
    except BaseException as e:
        logger_slack.error("=== Update counter forced failed", stack_info=True, exc_info=True)
        raise e
