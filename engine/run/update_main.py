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
    currency,
    destination,
    berth,
    entsog,
    rscript,
    trajectory,
    flaring,
)

import integrity
import base
from base.logger import logger_slack

import datetime as dt

import set_rlimit as _


def update():
    currency.update()

    kpler_scraper.update_full()
    kpler_trade_computed.update()

    ship.update()

    position.update(date_from=dt.date.today() - dt.timedelta(days=90))
    destination.update()
    berth.update()

    entsog.update(date_from=-21, nodata_error_date_from=-4)
    rscript.update()
    trajectory.update()

    alert.update()

    counter.update()
    counter.update(version=base.COUNTER_VERSION1)
    counter.update(version=base.COUNTER_VERSION2)

    backuper.update()
    integrity.check()

    flaring.update()

    return


if __name__ == "__main__":
    logger_slack.info("=== Full update: using %s environment ===" % (base.db.environment,))
    try:
        update()
        logger_slack.info("=== Full update complete ===")
    except BaseException as e:
        logger_slack.error("=== Full update failed", stack_info=True, exc_info=True)
        raise e
