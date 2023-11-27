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

import set_rlimit as _


def update():
    kpler_scraper.update(
        date_from="2021-01-01",
        origin_iso2s=["RU"],
    )
    kpler_trade_computed.update()

    counter.update()
    counter.update(version=base.COUNTER_VERSION1)
    counter.update(version=base.COUNTER_VERSION2)
    backuper.update()
    integrity.check()
    return


if __name__ == "__main__":
    logger_slack.info(
        "=== Update historic Russia: using %s environment ===" % (base.db.environment,)
    )
    try:
        update()
        logger_slack.info("=== Update historic Russia complete ===")
    except BaseException as e:
        logger_slack.error("=== Update historic Russia failed", stack_info=True, exc_info=True)
        raise e
