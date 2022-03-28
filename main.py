from engine import port
from engine import portcall
from engine import departure
from engine import arrival
from engine import flow
from base.db import init_db


def init():
    # init_db(drop_first=False)
    # portcall.fill_arrival_gaps(date_from='2021-12-01')
    # portcall.update_departures()
    # departure.update()
    # arrival.update(min_dwt=5000)
    # flow.update()
    flow.update_positions()
    return


if __name__ == "__main__":
    init()
