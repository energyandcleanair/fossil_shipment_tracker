from engine import port
from engine import portcall
from engine import departure
from engine import arrival
from engine import flow
from engine import berth
from base.db import init_db
import base

def init():
    init_db(drop_first=False)
    # portcall.fill_arrival_gaps(date_from='2021-12-01')
    portcall.fill_departure_gaps(date_from='2022-03-02')
    # portcall.update_departures()
    # portcall.fill_missing_port_operation()
    # portcall.fill_missing_port_operation()
    # departure.update()
    # arrival.update()
    # arrival.update(min_dwt=5000, commodities=[base.BULK])
    # flow.update()
    # flow.update_positions()
    # berth.detect_arrival_berths()
    return


if __name__ == "__main__":
    init()
