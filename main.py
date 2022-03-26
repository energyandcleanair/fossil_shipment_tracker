from engine import port
from engine import portcall
from engine import departure
from engine import arrival
from engine import flow
from base.db import init_db

def init():
    init_db(drop_first=False)
    # portcall.fill()
    # departure.update()
    arrival.update(min_dwt=150000)
    flow.update()
    return


if __name__ == "__main__":
    init()
