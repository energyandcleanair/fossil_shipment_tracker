from engines import (
    commodity,
    company,
    comtrade,
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
    currency.update()
    comtrade.update_comtrade_data(
        sync_definitions=comtrade.create_sync_definitions_for_all(
            start=dt.date(2020, 1, 1), end=dt.date.today()
        )
    )

    engine_r.update()

    commodity.fill()
    kpler_scraper.update_full()
    kpler_scraper.update(historic_date_from="2017-01-01", origin_iso2s=["ID"])
    company.update()
    kpler_trade_computed.update()

    entsog.update(date_from=-21, nodata_error_date_from=-4)

    counter.update()

    backuper.update()
    integrity.check()

    alert.update()

    return


if __name__ == "__main__":
    logger_slack.info("=== Full update: using %s environment ===" % (base.db.environment,))
    try:
        update()
        logger_slack.info("=== Full update complete ===")
    except BaseException as e:
        logger_slack.error("=== Full update failed", stack_info=True, exc_info=True)
        raise e
