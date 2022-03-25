import datetime as dt
from models import PortCall

from engine.marinetraffic import Marinetraffic


def test_query_portcall():
    # This will cost few credits each time...
    # We took an actual port call from Russia
    date_from = dt.datetime.strptime("2022-03-15 19:33:00", "%Y-%m-%d %H:%M:%S")
    portcall = Marinetraffic.get_first_arrival_portcall(imo="9776755", date_from=date_from)
    assert isinstance(portcall, PortCall)
    assert portcall.port_unlocode is None

    filter = lambda x: x.port_unlocode is not None
    portcall2 = Marinetraffic.get_first_arrival_portcall(imo="9776755", date_from=date_from, filter=filter)
    assert portcall2.port_unlocode is not None

