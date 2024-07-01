from engines import (
    commodity,
    engine_r,
    ship,
    counter,
    alert,
    backuper,
    kpler_scraper,
    kpler_trade_computed,
    currency,
    entsog,
    flaring,
)

import integrity
import base
from base.logger import logger_slack

import datetime as dt


def update():
    currency.update()
    engine_r.update()

    commodity.fill()
    kpler_scraper.update_full()
    kpler_trade_computed.update()

    entsog.update(date_from=-21, nodata_error_date_from=-4)

    alert.update()

    counter.update()

    backuper.update()
    integrity.check()

    return


if __name__ == "__main__":
    logger_slack.info("=== Full update: using %s environment ===" % (base.db.environment,))
    try:
        update()
        logger_slack.info("=== Full update complete ===")
    except BaseException as e:
        logger_slack.error("=== Full update failed", stack_info=True, exc_info=True)
        raise e
