from engines import (
    commodity,
    engine_r,
    counter,
    alert,
    backuper,
    kpler_scraper,
    kpler_trade_computed,
    currency,
    entsog,
)

import integrity
import base
from base.logger import logger_slack

import datetime as dt


def update():
    kpler_scraper.update(
        historic_date_from="2017-01-01",
        origin_iso2s=["ID"],
    )
    kpler_trade_computed.update()

    counter.update()
    integrity.check()

    return


if __name__ == "__main__":
    logger_slack.info("=== Update indonesia: using %s environment ===" % (base.db.environment,))
    try:
        update()
        logger_slack.info("=== Update indonesia complete ===")
    except BaseException as e:
        logger_slack.error("=== Update indonesia failed", stack_info=True, exc_info=True)
        raise e
