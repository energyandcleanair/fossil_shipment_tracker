from engines import (
    commodity,
    counter,
    kpler_scraper,
    kpler_trade_computed,
)

from engines.kpler_scraper import clean_outdated_entries
import integrity
import base
from base.logger import logger_slack

import datetime as dt


def update():
    commodity.fill()
    kpler_scraper.update(
        origin_iso2s=["RU"],
    )
    kpler_trade_computed.update()
    counter.update()
    integrity.check()
    return


if __name__ == "__main__":
    logger_slack.info("=== Update kpler: using %s environment ===" % (base.db.environment,))
    try:
        update()
        logger_slack.info("=== Update kpler complete ===")
    except BaseException as e:
        logger_slack.error("=== Update kpler failed", stack_info=True, exc_info=True)
        raise e
