from engines import (
    counter,
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

    kpler_scraper.update(
        origin_iso2s=["RU"],
        parts=[
            kpler_scraper.UpdateParts.UPDATE_RECENT_TRADES,
        ],
    )
    kpler_trade_computed.update()

    counter.update()
    backuper.update()
    integrity.check()
    return


if __name__ == "__main__":
    logger_slack.info("=== Update recent Russia: using %s environment ===" % (base.db.environment,))
    try:
        update()
        logger_slack.info("=== Update recent Russia complete ===")
    except BaseException as e:
        logger_slack.error("=== Update recent Russia failed", stack_info=True, exc_info=True)
        raise e
