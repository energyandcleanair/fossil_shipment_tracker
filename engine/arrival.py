import datetime as dt
import sqlalchemy
from sqlalchemy import func
from sqlalchemy.types import DateTime, VARCHAR, String
from sqlalchemy.sql.expression import cast
from tqdm import tqdm

import base
from engine import departure
from engine import portcall
from engine import shipment
from engine.marinetraffic import Marinetraffic
from base.logger import logger_slack
from base.db import session
from base.models import Arrival, Shipment, ShipmentWithSTS, Ship, MarineTrafficCall, Departure
from base.utils import to_list, to_datetime


def get_dangling_arrivals():
    subquery = session.query(Shipment.arrival_id).filter(Shipment.arrival_id != sqlalchemy.null())
    return Arrival.query.filter(~Arrival.id.in_(subquery)).all()


def update(
    min_dwt=base.DWT_MIN,
    limit=None,
    date_from="2022-01-01",
    date_to=None,
    commodities=None,
    ship_imo=None,
    unlocode=None,
    port_id=None,
    departure_port_iso2=None,
    shipment_id=None,
    force_for_arrival_to_next_portcall_greater_than=None,
    force_for_arrival_to_prev_portcall_greater_than=None,
    include_undetected_arrival_shipments=True,
    cache_only=False,
    exclude_sts=False,
    use_call_based=False,
):
    """

    :param min_dwt:
    :param limit:
    :param date_from:
    :param date_to:
    :param commodities:
    :param ship_imo:
    :param unlocode:
    :param port_id:
    :param force_for_arrival_to_departure_greater_than:
    :param include_undetected_arrival_shipments:
    :param cache_only:
    :return:
    """

    logger_slack.info("=== Arrival update ===")

    # We take dangling departures, and try to find the next arrival
    # As in, the arrival before the next relevant departure (i.e. with discharge)
    dangling_departures_query = departure.get_departures_without_arrival(
        min_dwt=min_dwt,
        commodities=commodities,
        date_from=date_from,
        date_to=date_to,
        ship_imo=ship_imo,
        unlocode=unlocode,
        port_id=port_id,
        departure_port_iso2=departure_port_iso2,
        shipment_id=shipment_id,
    )

    dangling_departures = session.query(dangling_departures_query).all()

    if not include_undetected_arrival_shipments:
        undetected_arrival_departures = (
            session.query(Shipment.departure_id)
            .filter(Shipment.status == base.UNDETECTED_ARRIVAL)
            .union(
                session.query(ShipmentWithSTS.departure_id).filter(
                    ShipmentWithSTS.status == base.UNDETECTED_ARRIVAL
                )
            )
            .all()
        )
        dangling_departures = [
            x
            for x in dangling_departures
            if x.id not in [y[0] for y in undetected_arrival_departures]
        ]

    if force_for_arrival_to_next_portcall_greater_than is not None:
        dangling_departures.extend(
            departure.get_departures_with_gap_around_arrival(
                min_gap_after=force_for_arrival_to_next_portcall_greater_than,
                min_dwt=min_dwt,
                commodities=commodities,
                date_from=date_from,
                date_to=date_to,
                ship_imo=ship_imo,
                unlocode=unlocode,
            )
        )

    if force_for_arrival_to_prev_portcall_greater_than is not None:
        dangling_departures.extend(
            departure.get_departures_with_gap_around_arrival(
                min_gap_before=force_for_arrival_to_prev_portcall_greater_than,
                min_dwt=min_dwt,
                commodities=commodities,
                date_from=date_from,
                date_to=date_to,
                ship_imo=ship_imo,
                unlocode=unlocode,
            )
        )

    if limit is not None:
        # For debugging without taking too many credits
        dangling_departures = dangling_departures[0:limit]

    # Very important to sort them by date, so that we don't miss any arrival
    # That would happen if a ship had two departure without yet an arrival
    # and we'd start looking from the latest departure
    dangling_departures.sort(key=lambda x: x.date_utc)

    # Temporary. Actually we do look between all portcalls, so the order
    # shouldn't really matter anymore.
    # Until we fix arrival detection, the first hundreds of dangling departures
    # will take lot of time for not much
    # dangling_departures.sort(key=lambda x: x.date_utc, reverse=True)

    if exclude_sts:
        dangling_departures = [x for x in dangling_departures if x.event_id is None]

    if use_call_based:

        latest_mtcall = (
            session.query(
                MarineTrafficCall.params["imo"].astext.cast(String).label("ship_imo"),
                func.max(MarineTrafficCall.params["todate"].astext.cast(DateTime)).label(
                    "max_queried_date"
                ),
            )
            .filter(MarineTrafficCall.status == base.HTTP_OK)
            .filter(
                sqlalchemy.or_(
                    MarineTrafficCall.params["movetype"].astext == "0",
                    sqlalchemy.not_(MarineTrafficCall.params.has_key("movetype")),
                )
            )
            .filter(MarineTrafficCall.method == base.VESSEL_PORTCALLS)
            .filter(
                MarineTrafficCall.params["imo"].astext.in_(
                    [x.ship_imo for x in dangling_departures]
                )
            )
            .group_by(MarineTrafficCall.params["imo"].astext)
            .subquery()
        )

        shipments = shipment.return_combined_shipments(session)

        latest_shipment = (
            session.query(
                Departure.ship_imo.label("ship_imo"),
                func.max(Arrival.date_utc).label("max_arrival_date"),
            )
            .join(shipments, shipments.c.shipment_departure_id == Departure.id)
            .join(Arrival, Arrival.id == shipments.c.shipment_arrival_id)
            .filter(
                shipments.c.shipment_status == base.COMPLETED,
                Departure.ship_imo.in_([x.ship_imo for x in dangling_departures]),
            )
            .group_by(Departure.ship_imo)
            .subquery()
        )

        dangling_departures_query = (
            session.query(
                dangling_departures_query.c.ship_imo,
                func.min(dangling_departures_query.c.date_utc).label("min_departure_date"),
            )
            .group_by(dangling_departures_query.c.ship_imo)
            .subquery()
        )

        dangling_departures = (
            session.query(
                dangling_departures_query.c.ship_imo,
                dangling_departures_query.c.min_departure_date,
                latest_mtcall.c.max_queried_date,
                latest_shipment.c.max_arrival_date,
            )
            .outerjoin(
                latest_mtcall, latest_mtcall.c.ship_imo == dangling_departures_query.c.ship_imo
            )
            .outerjoin(
                latest_shipment, latest_shipment.c.ship_imo == dangling_departures_query.c.ship_imo
            )
            .all()
        )

        for d in dangling_departures:
            date_from = max(
                [
                    d
                    for d in [
                        to_datetime(d.min_departure_date),
                        to_datetime(d.max_queried_date),
                        to_datetime(d.max_arrival_date),
                        to_datetime(dt.date.today() - dt.timedelta(days=90)),
                    ]
                    if d is not None
                ]
            )
            date_to = to_datetime(dt.date.today())

            if date_from >= date_to:
                continue

            portcalls = Marinetraffic.get_portcalls_between_dates(
                date_from=date_from,
                date_to=date_to,
                arrival_or_departure=None,
                imo=d.ship_imo,
                use_call_based=True,
            )

            portcall.upload_portcalls(portcalls)

    else:
        for d in tqdm(dangling_departures):
            arrival_portcall = portcall.find_arrival(departure=d, cache_only=cache_only)
