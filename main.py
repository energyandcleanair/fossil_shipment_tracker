from engine import port
from engine import portcall
from engine import departure
from engine import arrival
from engine import flow
from engine import trajectory
from engine import position
from engine import destination
from engine import berth
from base.db import init_db
import base

import datetime as dt

def update():
    # portcall.update_departures_from_russia()
    # portcall.fill_departure_gaps(date_from="2021-12-01", date_to="2022-01-01")
    # departure.update(ship_imo='9248552')
    # departure.update(unlocode=['RUVYP', 'RUULU', 'RUMMK', 'RULGA', 'RUVNN'], commodities=base.GENERAL_CARGO)
    arrival.update(force_for_arrival_to_departure_greater_than=dt.timedelta(hours=24*10))
    flow.update(date_from="2021-12-01")
    position.update()
    destination.update()
    berth.update()
    trajectory.update()
    return


if __name__ == "__main__":
    update()
