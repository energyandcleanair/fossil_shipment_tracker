"This fills departure table using MarineTraffic PortCall data"
from base.db import session
from base.db_utils import upsert

from models import PortCall, Departure, Arrival


def get_dangling_departures():
    subquery = session.query(Arrival.departure_id)
    return Departure.query.filter(~Departure.id.in_(subquery)).all()


def get_dangling_imos():
    subquery = session.query(Arrival.departure_id)
    return session.query(Departure.ship_imo).filter(~Departure.id.in_(subquery)).all()


def update():

    # Look for PortCalls without associated departure
    subquery = session.query(Departure.portcall_id)
    dangling_portcalls = PortCall.query.filter(~PortCall.id.in_(subquery)).all()

    # For all those dangling portcalls.
    # we create a Departure
    for pc in dangling_portcalls:
        departure_data = {
            "port_unlocode": pc.port_unlocode,
            "ship_imo": pc.ship_imo,
            "date_utc": pc.date_utc,
            "portcall_id": pc.id,
            "method_id": "from_marinetraffic_portcall"
        }
        session.add(Departure(**departure_data))
    session.commit()










def update_from_datalastic():
    pass