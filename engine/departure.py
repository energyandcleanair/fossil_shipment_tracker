import datetime as dt

from base.db import session
from base.utils import to_datetime, to_list
import base
from base.logger import logger_slack
import sqlalchemy as sa
from sqlalchemy import func
from base.models import PortCall, Departure, Arrival, Ship, Port, Shipment, Event, ShipmentWithSTS
from engine import shipment



def get_departures_with_gap_around_arrival(min_gap_before=None,
                                           min_gap_after=None,
                                           min_dwt=None,
                                           commodities=None,
                                           date_from=None,
                                           ship_imo=None,
                                           date_to=None,
                                           unlocode=None):

    portcall_next = session.query(PortCall.id,
                                  func.lead(PortCall.date_utc).over(
                                      partition_by=PortCall.ship_imo,
                                      order_by=PortCall.date_utc).label('next_date_utc'),
                                  func.lead(PortCall.date_utc).over(
                                      partition_by=PortCall.ship_imo,
                                      order_by=PortCall.date_utc.desc()).label('prev_date_utc')
                                  ).subquery()

    subq1 = session.query(Departure,
                          portcall_next,
                          Arrival.date_utc.label('arrival_date_utc')) \
        .join(Arrival, Arrival.departure_id == Departure.id) \
        .join(portcall_next, portcall_next.c.id == Arrival.portcall_id) \
        .subquery()

    query = session.query(Departure) \
        .join(subq1, Departure.id == subq1.c.id) \
        .join(Ship, Departure.ship_imo == Ship.imo) \
        .join(Port, Departure.port_id == Port.id)

    if min_gap_before:
        query = query.filter(subq1.c.arrival_date_utc - subq1.c.prev_date_utc >= min_gap_before)

    if min_gap_after:
        query = query.filter(subq1.c.next_date_utc - subq1.c.arrival_date_utc >= min_gap_after)

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

    if unlocode is not None:
        query = query.filter(Port.unlocode.in_(to_list(unlocode)))

    return query.order_by(Departure.date_utc).all()



def get_departures_without_arrival(min_dwt=None, commodities=None,
                                   date_from=None, ship_imo=None, date_to=None,
                                   unlocode=None, port_id=None, departure_port_iso2=None, shipment_id=None):

    subquery = session.query(Arrival.departure_id).filter(Arrival.departure_id != sa.null())

    shipments = shipment.return_combined_shipments(session)

    query = session.query(Departure).filter(~Departure.id.in_(subquery)) \
        .outerjoin(shipments, shipments.c.shipment_departure_id == Departure.id) \
        .join(Ship, Departure.ship_imo == Ship.imo) \
        .join(Port, Departure.port_id == Port.id, isouter=True)

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

    if shipment_id is not None:
        query = query.filter(shipments.c.shipment_id.in_(to_list(shipment_id)))

    if unlocode is not None:
        query = query.filter(Port.unlocode.in_(to_list(unlocode)))

    if port_id is not None:
        query = query.filter(Port.id.in_(to_list(port_id)))

    if departure_port_iso2 is not None:
        query = query.filter(Port.iso2.in_(to_list(departure_port_iso2)))

    return query.order_by(Departure.date_utc).all()


def get_departures_without_shipment(min_dwt=None, commodities=None, date_from=None, ship_imo=None):
    subquery = session.query(Shipment.departure_id).filter(Shipment.departure_id != sa.null())
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


def update(date_from="2022-01-01"):

    add(date_from=date_from, commodities=[base.LNG, base.CRUDE_OIL, base.OIL_PRODUCTS,
                           base.OIL_OR_CHEMICAL, base.COAL, base.BULK])

    add(date_from=date_from, unlocode=['RUVYP', 'RUULU', 'RUMMK', 'RULGA', 'RUVNN', 'RUAZO'],
                     commodities=base.GENERAL_CARGO)

    # Only keep oil related for India
    remove(unlocode=['INSIK'],
           commodities=[base.LNG, base.COAL, base.BULK])

    remove(port_name='SIKKA ANCH',
           commodities=[base.LNG, base.COAL, base.BULK])

    remove(unlocode=['EGMAH'],
           commodities=[base.LNG, base.COAL, base.BULK])

    remove(port_name='MERSA EL HAMRA ANCH',
           commodities=[base.LNG, base.COAL, base.BULK])


def add(date_from="2022-01-01",
           min_dwt=base.DWT_MIN,
           limit=None,
           commodities=[base.LNG,
                        base.CRUDE_OIL,
                        base.OIL_PRODUCTS,
                        base.OIL_OR_CHEMICAL,
                        base.COAL,
                        base.BULK],
           ship_imo=None,
           unlocode=None,
           port_id=None,
           marinetraffic_id=None,
           ):
    logger_slack.info("=== Update departures ===")
    # Look for relevant PortCalls without associated departure
    subquery = session.query(Departure.portcall_id).filter(Departure.portcall_id != sa.null())

    dangling_portcalls = session.query(PortCall).filter(
        PortCall.move_type == "departure",
        PortCall.load_status.in_([base.FULLY_LADEN]),
        PortCall.port_operation.in_(["load"]),
        sa.not_(PortCall.id.in_(subquery)),
        Port.check_departure) \
        .join(Ship, PortCall.ship_imo == Ship.imo) \
        .join(Port, PortCall.port_id == Port.id)

    if commodities:
        dangling_portcalls = dangling_portcalls.filter(Ship.commodity.in_(to_list(commodities)))

    if min_dwt is not None:
        dangling_portcalls = dangling_portcalls.filter(Ship.dwt >= min_dwt)

    if date_from is not None:
        dangling_portcalls = dangling_portcalls.filter(PortCall.date_utc >= to_datetime(date_from))

    if ship_imo is not None:
        dangling_portcalls = dangling_portcalls.filter(PortCall.ship_imo.in_(to_list(ship_imo)))

    if unlocode is not None:
        dangling_portcalls = dangling_portcalls.filter(Port.unlocode.in_(to_list(unlocode)))

    if port_id is not None:
        dangling_portcalls = dangling_portcalls.filter(Port.id.in_(to_list(port_id)))

    if marinetraffic_id is not None:
        dangling_portcalls = dangling_portcalls.filter(Port.marinetraffic_id.in_(to_list(marinetraffic_id)))

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


def remove(commodities=None, unlocode=None, port_id=None, port_name=None, marinetraffic_id=None,
           date_to=None):

    departures = session.query(Departure.id) \
        .join(Ship, Ship.imo==Departure.ship_imo) \
        .join(PortCall, PortCall.id == Departure.portcall_id) \
        .join(Port, PortCall.port_id == Port.id) \
        .outerjoin(Shipment, Shipment.departure_id == Departure.id) \
        .filter(Shipment.id == sa.null())

    if commodities:
        departures = departures \
            .filter(Ship.commodity.in_(to_list(commodities)))

    if unlocode:
        departures = departures \
         .filter(Port.unlocode.in_(to_list(unlocode)))

    if port_id:
        departures = departures \
            .filter(Port.id.in_(to_list(port_id)))

    if marinetraffic_id:
        departures = departures.filter(
            Port.marinetraffic_id.in_(to_list(marinetraffic_id)))

    if port_name:
        departures = departures \
            .filter(Port.name.in_(to_list(port_name)))

    if date_to:
        departures = departures \
            .filter(Departure.date_utc <= to_datetime(date_to))

    session.query(Departure) \
        .filter(Departure.id.in_(departures.scalar_subquery().subquery())) \
        .delete(synchronize_session=False)

    session.commit()