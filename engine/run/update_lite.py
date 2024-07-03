from engines import (
    company,
    comtrade,
    engine_r,
    currency,
    counter,
    entsog,
    kpler_scraper,
    kpler_trade_computed,
)

import integrity
import base
from base.logger import logger_slack

import datetime as dt


def update():
    currency.update()
    comtrade.update_comtrade_data(
        sync_definitions=comtrade.create_sync_definitions_for_all(
            start=dt.date(2020, 1, 1), end=dt.date.today()
        )
    )

    engine_r.update()

    kpler_scraper.update_lite()
    company.update()
    kpler_trade_computed.update()

    entsog.update(date_from=-21, nodata_error_date_from=-4)

    counter.update()
    integrity.check()

    return


if __name__ == "__main__":
    logger_slack.info("=== Lite update: using %s environment ===" % (base.db.environment,))
    try:
        update()
        logger_slack.info("=== Lite update complete ===")
    except BaseException as e:
        logger_slack.error("=== Lite update failed", stack_info=True, exc_info=True)
        raise e
