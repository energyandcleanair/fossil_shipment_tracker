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
from engine import entsog
from base.db import init_db
import base

import datetime as dt


def update():
    # ship.fix_mmsi_imo_discrepancy(date_from="2022-01-01")
    # ship.fill_missing_commodity()
    # port.add_check_departure_to_anchorage()
    # portcall.update_departures_from_russia(date_from='2022-04-10',
    #                                         force_rebuild=True)
    # portcall.fill_departure_gaps(date_from='2022-04-10')
    portcall.update_departures_from_russia(date_from='2022-01-01')
    # # #
    # # # # portcall.fill_departure_gaps(date_from="2022-04-10", unlocode='RUULU')
    departure.update(commodities=[base.LNG, base.CRUDE_OIL, base.OIL_PRODUCTS,
                           base.OIL_OR_CHEMICAL, base.COAL, base.BULK])

    departure.update(unlocode=['RUVYP', 'RUULU', 'RUMMK', 'RULGA', 'RUVNN', 'RUAZO'],
                     commodities=base.GENERAL_CARGO)
    #
    # # # # arrival.update(force_for_arrival_to_departure_greater_than=dt.timedelta(hours=24*10))
    arrival.update(date_from="2022-01-01", include_undetected_arrival_shipments=False)
    arrival.update(date_from="2022-04-01", include_undetected_arrival_shipments=True)
    shipment.update()
    # destination.update()
    # berth.update()
    counter.update()
    position.update()
    trajectory.update()
    counter.update()
    return


if __name__ == "__main__":
    # from base.db import init_db
    # init_db()
    # commodity.fill()
    # country.fill()
    print("=== Using %s environment ===" %(base.db.environment,))
    update()
