from engine import port
from engine import portcall
from engine import departure
from engine import arrival
from engine import flow
from base.db import init_db


def init():
    # init_db(drop_first=False)
    portcall.update_departures(date_from="2021-09-01",
                               date_to="2021-12-01",
                               force_rebuild=True)
    departure.update()
    arrival.update(min_dwt=5000)
    flow.update()
    return


if __name__ == "__main__":
    init()
