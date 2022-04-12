"This fills departure table using MarineTraffic PortCall data"
from base.db import session
from base.utils import to_datetime, to_list
import base
import sqlalchemy as sa
from base.models import PortCall, Departure, Arrival, Ship, Port, Flow


def get_departures_without_arrival(min_dwt=None, commodities=None, date_from=None, ship_imo=None, date_to=None):
    subquery = session.query(Arrival.departure_id).filter(Arrival.departure_id != sa.null())
    query = session.query(Departure).filter(~Departure.id.in_(subquery)) \
        .join(PortCall, PortCall.id == Departure.portcall_id) \
        .join(Ship, PortCall.ship_imo == Ship.imo)

    if min_dwt is not None:
        query = query.filter(Ship.dwt >= min_dwt)

    if date_from is not None:
        query = query.filter(Departure.date_utc >= to_datetime(date_from))

    if date_to is not None:
        query = query.filter(Departure.date_utc <= to_datetime(date_to))

    if commodities is not None:
        query = query.filter(Ship.commodity.in_(to_list(commodities)))

    if ship_imo is not None:
        query = query.filter(Ship.imo.in_(to_list(ship_imo)))

    return query.order_by(Departure.date_utc).all()


def get_departures_without_flow(min_dwt=None, commodities=None, date_from=None, ship_imo=None):
    subquery = session.query(Flow.departure_id).filter(Flow.departure_id != sa.null())
    query = session.query(Departure).filter(~Departure.id.in_(subquery)) \
        .join(PortCall, PortCall.id == Departure.portcall_id) \
        .join(Ship, PortCall.ship_imo == Ship.imo)

    if min_dwt is not None:
        query = query.filter(Ship.dwt >= min_dwt)

    if date_from is not None:
        query = query.filter(Departure.date_utc >= to_datetime(date_from))

    if commodities is not None:
        query = query.filter(Ship.commodity.in_(to_list(commodities)))

    if ship_imo is not None:
        query = query.filter(Ship.imo.in_(to_list(ship_imo)))

    return query.order_by(Departure.date_utc).all()


def get_dangling_imos():
    subquery = session.query(Arrival.departure_id)
    return session.query(Departure.ship_imo).filter(~Departure.id.in_(subquery)).all()


def get_dangling_imo_dates():
    subquery = session.query(Arrival.departure_id)
    return session.query(Departure.ship_imo, Departure.date_utc).filter(~Departure.id.in_(subquery)).all()


def update(date_from="2022-01-01",
           min_dwt=base.DWT_MIN,
           limit=None,
           commodities=[base.LNG,
                        base.CRUDE_OIL,
                        base.OIL_PRODUCTS,
                        base.OIL_OR_CHEMICAL,
                        base.COAL,
                        base.BULK]
           ):
    print("=== Update departures ===")
    # Look for relevant PortCalls without associated departure
    subquery_ports = session.query(Port.id).filter(Port.check_departure)
    subquery = session.query(Departure.portcall_id)

    dangling_portcalls = PortCall.query.filter(
        PortCall.move_type == "departure",
        PortCall.load_status.in_([base.FULLY_LADEN]),
        PortCall.port_operation.in_(["load"]),
        ~PortCall.id.in_(subquery),
        PortCall.port_id.in_(subquery_ports)) \
        .join(Ship, PortCall.ship_imo == Ship.imo)

    if commodities:
        dangling_portcalls = dangling_portcalls.filter(Ship.commodity.in_(commodities))

    if min_dwt is not None:
        dangling_portcalls = dangling_portcalls.filter(Ship.dwt >= min_dwt)

    if date_from is not None:
        dangling_portcalls = dangling_portcalls.filter(PortCall.date_utc >= to_datetime(date_from))

    dangling_portcalls = dangling_portcalls.all()

    if limit is not None:
        dangling_portcalls = dangling_portcalls[0:limit]

    # For all those dangling portcalls.
    # we create a Departure
    for pc in dangling_portcalls:
        departure_data = {
            "port_id": pc.port_id,
            "ship_imo": pc.ship_imo,
            "date_utc": pc.date_utc,
            "portcall_id": pc.id,
            "method_id": "from_marinetraffic_portcall"
        }
        session.add(Departure(**departure_data))
    session.commit()
