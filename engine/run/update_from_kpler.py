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

import datetime as dt


def update():
    kpler_scraper.update_full()
    kpler_trade_computed.update()
    alert.update()
    counter.update()
    counter.update(version=base.COUNTER_VERSION1)
    counter.update(version=base.COUNTER_VERSION2)
    backuper.update()
    integrity.check()
    return


if __name__ == "__main__":
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
