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
from engine import rscript
from engine import counter
from engine import entsog
import base

import datetime as dt


def update():
    portcall.update_departures_from_russia()
    ship.update()
    departure.update(commodities=[base.LNG, base.CRUDE_OIL, base.OIL_PRODUCTS,
                           base.OIL_OR_CHEMICAL, base.COAL, base.BULK])

    departure.update(unlocode=['RUVYP', 'RUULU', 'RUMMK', 'RULGA', 'RUVNN', 'RUAZO'],
                     commodities=base.GENERAL_CARGO)

    # Only keep oil related for India
    departure.remove(unlocode=['INSIK'],
                     port_id=114313,
                     commodities=[base.LNG, base.COAL, base.BULK])

    departure.remove(port_name='SIKKA ANCH',
                     commodities=[base.LNG, base.COAL, base.BULK])

    arrival.update(date_from = dt.date.today() - dt.timedelta(days=90))
    shipment.update()
    position.update()
    destination.update()
    berth.update()
    entsog.update(date_from=-14)
    rscript.update()
    trajectory.update()
    counter.update()
    return



if __name__ == "__main__":
    print("=== Using %s environment ===" %(base.db.environment,))
    update()
