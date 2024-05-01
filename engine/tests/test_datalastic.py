import datetime as dt
from base.models import Position
import pytest

from engines.datalastic import default_datalastic


@pytest.mark.system
def test_query_ship():
    ship = default_datalastic.get_ship(mmsi="538008212", use_cache=False)
    ship_cached = default_datalastic.get_ship(mmsi="538008212", use_cache=True)

    assert ship.mmsi == ship_cached.mmsi and ship.imo == ship_cached.imo


@pytest.mark.system
def test_find_position():
    date_str = "2022-05-23T08:49:00"
    position = default_datalastic.get_position(imo=9723590, date=date_str)
    print(position.geometry, position.date_utc)
    return


@pytest.mark.system
def test_find_ship():
    ship1 = default_datalastic.find_ship(name="YANG MEI HU", fuzzy=False, return_closest=False)
    ship2 = default_datalastic.find_ship(name="YANG MEI HU", fuzzy=True, return_closest=True)
    ship3 = default_datalastic.find_ship(name="YANG MEI HU", fuzzy=True, return_closest=False)

    assert ship1 and ship2 and ship3

    assert ship1[0].name == ship2[0].name == ship3[0].name


@pytest.mark.system
def test_query_position():
    # This will cost few credits each time...
    # We took an actual port call from Russia
    date_from = dt.datetime.strptime("2022-03-15 19:33:00", "%Y-%m-%d %H:%M:%S")
    positions = default_datalastic.get_positions(
        imo="9776755", date_from=date_from, date_to=dt.datetime.now()
    )
    assert len(positions) > 0
    assert all([isinstance(x, Position) for x in positions])
