from engines import (
    trajectory,
    position,
    destination,
    berth,
    currency,
    rscript,
    counter,
    entsog,
    flaring,
    kpler_scraper,
    kpler_trade_computed,
)

import integrity
import base
from base.logger import logger_slack

import datetime as dt


def update():
    # integrity.check()
    # portcall.update_departures(departure_port_iso2=['RU'],
    #                           date_from=-14,
    #                           force_rebuild=True,
    #                           between_existing_only=True)
    # ship.update()
    # departure.update()
    # arrival.update(date_from=dt.date.today() - dt.timedelta(days=90), departure_port_iso2=['RU'])
    currency.update()
    kpler_scraper.update_lite()
    kpler_trade_computed.update()

    # company.update()
    # mtevents.update(date_from=dt.date.today() - dt.timedelta(days=90))
    # sts.update(date_from=dt.date.today() - dt.timedelta(days=90))
    # shipment.update()
    position.update(date_from=dt.date.today() - dt.timedelta(days=90))
    destination.update()
    berth.update()
    # sts.update(date_from=dt.date.today() - dt.timedelta(days=90))
    entsog.update(date_from=-21, nodata_error_date_from=-4)
    rscript.update()
    trajectory.update()
    flaring.update()
    # alert.update()
    counter.update()
    counter.update(version=base.COUNTER_VERSION1)
    counter.update(version=base.COUNTER_VERSION2)
    integrity.check()
    return


if __name__ == "__main__":
    logger_slack.info("=== Lite update: using %s environment ===" % (base.db.environment,))
    update()
    logger_slack.info("=== Lite update complete ===")
