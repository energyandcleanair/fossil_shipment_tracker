from engines import (
    commodity,
    engine_r,
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
    trajectory,
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

    ship.update()

    position.update(date_from=dt.date.today() - dt.timedelta(days=90))
    destination.update()
    berth.update()

    entsog.update(date_from=-21, nodata_error_date_from=-4)
    trajectory.update()

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
