from engine import port
from engine import portcall
from engine import departure
from engine import arrival
from engine import shipment
from engine import trajectory
from engine import position
from engine import destination
from engine import berth
from engine import ship
from engine import currency
from engine import rscript
from engine import counter
from engine import entsog
from engine import alert
from engine import company
from engine import mtevents
from engine import flaring
from engine import sts
from engine import backuper
from engine import kpler_scraper

import integrity
import base

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
    alert.update()
    counter.update()
    counter.update(version=base.COUNTER_VERSION1)
    backuper.update()
    integrity.check()
    return


if __name__ == "__main__":
    print("=== Using %s environment ===" % (base.db.environment,))
    update()