import datetime as dt
from base.models import Position

from engine.datalastic import Datalastic


def test_query_ship():
    ship = Datalastic.get_ship(mmsi="538008212")



def test_query_position():
    # This will cost few credits each time...
    # We took an actual port call from Russia
    date_from = dt.datetime.strptime("2022-03-15 19:33:00", "%Y-%m-%d %H:%M:%S")
    positions = Datalastic.get_position(imo="9776755", date_from=date_from, date_to=dt.datetime.now())
    assert len(positions) > 0
    assert all([isinstance(x, Position) for x in positions])


