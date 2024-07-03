from engines import (
    commodity,
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

    commodity.fill()

    kpler_scraper.update(
        recent_date_from=None,
        recent_date_to=None,
        origin_iso2s=["RU"],
        parts=[
            kpler_scraper.UpdateParts.REFETCH_OUTDATED_HISTORIC_ENTRIES,
        ],
    )
    kpler_trade_computed.update()

    counter.update()
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
