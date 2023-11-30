from engines import (
    trajectory,
    position,
    destination,
    berth,
    currency,
    rscript,
    counter,
    entsog,
    flaring,
    kpler_scraper,
    kpler_trade_computed,
)

import integrity
import base
from base.logger import logger_slack

import datetime as dt

import set_rlimit as _


def update():
    kpler_trade_computed.update()

    counter.update()
    counter.update(version=base.COUNTER_VERSION1)
    counter.update(version=base.COUNTER_VERSION2)
    integrity.check()

    flaring.update()

    return


if __name__ == "__main__":
    logger_slack.info("=== Lite update: using %s environment ===" % (base.db.environment,))
    try:
        update()
        logger_slack.info("=== Lite update complete ===")
    except BaseException as e:
        logger_slack.error("=== Lite update failed", stack_info=True, exc_info=True)
        raise e
