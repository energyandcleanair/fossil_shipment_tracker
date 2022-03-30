import datetime as dt
from base.models import PortCall

from engine.marinetraffic import Marinetraffic


def test_query_portcall():
    # This will cost few credits each time...
    # We took an actual port call from Russia
    date_from = dt.datetime.strptime("2022-03-15 19:33:00", "%Y-%m-%d %H:%M:%S")
    portcall = Marinetraffic.get_next_arrival_portcall(imo="9776755", date_from=date_from)
    assert isinstance(portcall, PortCall)
    assert portcall.port_unlocode is None

    filter = lambda x: x.port_unlocode is not None
    portcall2 = Marinetraffic.get_next_arrival_portcall(imo="9776755", date_from=date_from, filter=filter)
    assert portcall2.port_unlocode is not None
    assert portcall2.date_utc > date_from

    portcall3 = Marinetraffic.get_next_arrival_portcall(imo="9776755", date_from=date_from, filter=filter, go_backward=True)
    assert portcall3.port_unlocode is not None
    assert portcall3.date_utc < date_from

