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
import integrity
import base

import datetime as dt


def update():
    integrity.check()
    portcall.update_departures_from_russia()
    ship.update()
    departure.update()
    arrival.update(date_from = dt.date.today() - dt.timedelta(days=90))
    currency.update()
    company.update()
    mtevents.update()
    shipment.update()
    position.update()
    destination.update()
    berth.update()
    entsog.update(date_from=-21, nodata_error_date_from=-4)
    rscript.update()
    trajectory.update()
    flaring.update()
    alert.update()
    counter.update()
    integrity.check()
    return



if __name__ == "__main__":
    print("=== Using %s environment ===" % (base.db.environment,))
    update()
