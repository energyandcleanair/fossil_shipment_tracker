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

from engines.kpler_scraper import clean_outdated_entries
import integrity
import base
from base.logger import logger_slack

import datetime as dt


def update():
    clean_outdated_entries()

    kpler_scraper.update(
        recent_date_from=-30,
        historic_date_from="2021-01-01",
        origin_iso2s=["RU"],
        platforms=["liquids"],
    )
    kpler_trade_computed.update()

    counter.update()
    counter.update(version=base.COUNTER_VERSION1)
    counter.update(version=base.COUNTER_VERSION2)

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
