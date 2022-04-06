from engine import port
from engine import portcall
from engine import departure
from engine import arrival
from engine import flow
from engine import trajectory
from engine import position
from engine import berth
from base.db import init_db
import base


def init():
    # init_db(drop_first=False)
    # port.fill()
    portcall.initial_fill()
    # berth.fill()
    # flow.rebuild()

    # portcall.fill_arrival_gaps(date_from='2021-12-01')
    # portcall.fill_departure_gaps(date_from='2022-01-01')
    # portcall.update_departures(date_from="2022-01-01", force_rebuild=True)
    # portcall.fill_missing_port_operation()
    # portcall.fill_missing_port_operation()
    # departure.update()
    # arrival.update(date_from="2022-01-01")
    # arrival.update(min_dwt=5000, commodities=[base.BULK])
    # flow.update()
    # flow.update_positions(commodities=base.BULK)
    # berth.detect_berths()
    # trajectory.update(rebuild_all=False)
    return


def update():
    # portcall.update_departures_from_russia()
    # departure.update()
    # arrival.update(date_from="2022-01-01")
    flow.update()
    # flow.rebuild()
    position.update()
    berth.detect_berths()
    trajectory.update()
    return


if __name__ == "__main__":
    update()
