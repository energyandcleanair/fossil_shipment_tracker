"This fills departure table using MarineTraffic PortCall data"
from base.db import session
from base.utils import to_datetime
import base
from base.models import PortCall, Departure, Arrival, Ship, Port


def get_dangling_departures(min_dwt=None, commodities=None, date_from=None):
    subquery = session.query(Arrival.departure_id)
    query = session.query(Departure).filter(~Departure.id.in_(subquery)).join(Ship)

    if min_dwt is not None:
        query = query.filter(Ship.dwt >= min_dwt)

    if date_from is not None:
        query = query.filter(Departure.date_utc >= to_datetime(date_from))

    if commodities is not None:
        query = query.filter(Ship.commodity.in_(commodities))

    return query.order_by(Departure.date_utc).all()


def get_dangling_imos():
    subquery = session.query(Arrival.departure_id)
    return session.query(Departure.ship_imo).filter(~Departure.id.in_(subquery)).all()


def get_dangling_imo_dates():
    subquery = session.query(Arrival.departure_id)
    return session.query(Departure.ship_imo, Departure.date_utc).filter(~Departure.id.in_(subquery)).all()


def update(date_from=None, limit=None):
    print("=== Update departures ===")
    # Look for relevant PortCalls without associated departure
    subquery_ports = session.query(Port.unlocode).filter(Port.check_departure)
    subquery = session.query(Departure.portcall_id)
    dangling_portcalls = PortCall.query.filter(
        PortCall.move_type == "departure",
        PortCall.load_status.in_(["fully_laden"]),
        PortCall.port_operation.in_(["load"]),
        ~PortCall.id.in_(subquery),
        PortCall.port_unlocode.in_(subquery_ports))\

    if date_from is not None:
        dangling_portcalls = dangling_portcalls.filter(PortCall.date_utc >= date_from)

    dangling_portcalls = dangling_portcalls.all()

    if limit is not None:
        dangling_portcalls = dangling_portcalls[0:limit]

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
