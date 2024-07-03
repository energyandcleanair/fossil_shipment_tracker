from engines import (
    kpler_scraper,
)

from engines.kpler_scraper import clean_outdated_entries
import integrity
import base
from base.logger import logger_slack

import datetime as dt


def verify():
    kpler_scraper.validate_sync(
        origin_iso2s=["RU"],
        date_from=dt.date(2021, 1, 1),
        date_to=dt.date.today() - dt.timedelta(days=1),
    )
    return


if __name__ == "__main__":
    logger_slack.info("=== Verifying kpler: using %s environment ===" % (base.db.environment,))
    try:
        verify()
        logger_slack.info("=== Verifying kpler complete ===")
    except BaseException as e:
        logger_slack.error("=== Verifying kpler failed", stack_info=True, exc_info=True)
        raise e
