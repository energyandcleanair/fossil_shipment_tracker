import datetime as dt
from base.models import Position

from engine.datalastic import Datalastic


def test_query_ship():
    ship = Datalastic.get_ship(mmsi="538008212")

def test_find_ship():
    ship1 = Datalastic.find_ship(name="YANG MEI HU", fuzzy=False, return_closest=False)
    ship2 = Datalastic.find_ship(name="YANG MEI HU", fuzzy=True, return_closest=True)
    ship3 = Datalastic.find_ship(name="YANG MEI HU", fuzzy=True, return_closest=False)

    assert ship1.name == ship2.name == ship3.name


def test_query_position():
    # This will cost few credits each time...
    # We took an actual port call from Russia
    date_from = dt.datetime.strptime("2022-03-15 19:33:00", "%Y-%m-%d %H:%M:%S")
    positions = Datalastic.get_positions(imo="9776755", date_from=date_from, date_to=dt.datetime.now())
    assert len(positions) > 0
    assert all([isinstance(x, Position) for x in positions])


