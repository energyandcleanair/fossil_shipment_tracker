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
from .. import base

import datetime as dt


def update():
    # integrity.check()
    # sts.check_multi_stage_sts()
    portcall.update_departures(
        departure_port_iso2=["RU"],
        date_from=-14,
        force_rebuild=True,
        use_call_based=True,
    )
    ship.update()
    departure.update()
    arrival.update(date_from=dt.date.today() - dt.timedelta(days=90), departure_port_iso2=["RU"])
    # currency.update()
    # company.update()
    # mtevents.update(date_from=dt.date.today() - dt.timedelta(days=90), only_for_ongoing_shipments=False)
    # sts.update(date_from=dt.date.today() - dt.timedelta(days=90))
    # sts.check_multi_stage_sts("2022-06-01")
    shipment.update()
    position.update(date_from=dt.date.today() - dt.timedelta(days=90))
    # destination.update()
    # berth.update()
    # entsog.update(date_from=-21, nodata_error_date_from=-4)
    # rscript.update()
    # trajectory.update()
    # flaring.update()
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
