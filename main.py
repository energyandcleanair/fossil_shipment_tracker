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


def update():
    # portcall.update_departures_from_russia(unlocode="FIKVH", date_from="2021-12-01")
    # portcall.fill_departure_gaps(date_from="2021-12-01", date_to="2022-01-01")
    # departure.update(date_from="2021-12-01")
    arrival.update()
    flow.update(date_from="2021-12-01")
    position.update()
    destination.update()
    berth.update()
    trajectory.update()
    return


if __name__ == "__main__":
    update()
