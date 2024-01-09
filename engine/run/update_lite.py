from engines import (
    engine_r,
    trajectory,
    position,
    destination,
    berth,
    currency,
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


def update():
    currency.update()

    engine_r.update()

    kpler_scraper.update_lite()
    kpler_trade_computed.update()

    position.update(date_from=dt.date.today() - dt.timedelta(days=90))
    destination.update()
    berth.update()

    entsog.update(date_from=-21, nodata_error_date_from=-4)

    trajectory.update()

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
