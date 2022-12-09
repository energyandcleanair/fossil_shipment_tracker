import datetime as dt
from base.models import PortCall

from engine import ship, port

from engine.marinetraffic import Marinetraffic

def test_get_ship_events():
    events = Marinetraffic.get_ship_events_between_dates(imo="9417177", date_from='2022-05-18', date_to='2022-05-24', use_cache=True, cache_objects=False)
    for e in events:
        assert e.ship_imo is not None and e.ship_name is not None and e.content is not None

def test_ship():
    mmsi='642122016'

    ship = Marinetraffic.get_ship(mmsi=mmsi, use_cache=True)
    assert ship.mmsi[0] == mmsi

    ship_noncached = Marinetraffic.get_ship(mmsi=mmsi, use_cache=False)
    assert ship_noncached.mmsi[0] == mmsi

def test_query_portcall():
    # This will cost few credits each time...
    # We took an actual port call from Russia
    date_from = dt.datetime.strptime("2022-03-15 19:33:00", "%Y-%m-%d %H:%M:%S")
    filtered_portcall, portcalls = Marinetraffic.get_next_portcall(imo="9776755", date_from=date_from, arrival_or_departure="arrival")
    assert isinstance(portcalls[0], PortCall)
    assert portcalls[0].port_id is not None

    filter = lambda x: x.port_id is not None
    filtered_portcall2, portcalls2 = Marinetraffic.get_next_portcall(imo="9776755", date_from=date_from, arrival_or_departure="arrival", filter=filter)
    assert portcalls2[0].port_id is not None
    assert dt.datetime.strptime(portcalls2[0].date_utc, "%Y-%m-%dT%H:%M:%S") > date_from

    filtered_portcall3, portcalls3 = Marinetraffic.get_next_portcall(imo="9776755", date_from=date_from, arrival_or_departure="arrival", filter=filter, go_backward=True)
    assert portcalls3[0].port_id is not None
    assert dt.datetime.strptime(portcalls3[0].date_utc, "%Y-%m-%dT%H:%M:%S") < date_from

    # Verify ship fill and unlocode filtering works
    filtered_portcalls, portcalls = Marinetraffic.get_portcalls_between_dates(date_from=dt.date.today() - dt.timedelta(days=1),
                                                                              date_to=dt.date.today() + dt.timedelta(days=1),
                                                                              unlocode='RUULU')