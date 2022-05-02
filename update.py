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
from engine import country
from engine import counter
from engine import commodity
from base.db import init_db
import base

import datetime as dt


def update():
    portcall.update_departures_from_russia()
    departure.update(commodities=[base.LNG, base.CRUDE_OIL, base.OIL_PRODUCTS,
                           base.OIL_OR_CHEMICAL, base.COAL, base.BULK])

    departure.update(unlocode=['RUVYP', 'RUULU', 'RUMMK', 'RULGA', 'RUVNN', 'RUAZO'],
                     commodities=base.GENERAL_CARGO)

    arrival.update(date_from = dt.date.today() - dt.timedelta(days=7))
    shipment.update()
    position.update()
    destination.update()
    berth.update()
    counter.update()
    return