import datetime as dt
import sqlalchemy
from tqdm import tqdm

import base
from engine import departure
from engine import portcall
from base.logger import logger
from base.db import session
from base.models import Arrival, Shipment, Port, PortCall, Departure, ShipmentArrivalBerth, Trajectory


def get_dangling_arrivals():
    subquery = session.query(Shipment.arrival_id).filter(Shipment.arrival_id != sqlalchemy.null())
    return Arrival.query.filter(~Arrival.id.in_(subquery)).all()


def update(min_dwt=base.DWT_MIN,
           limit=None,
           date_from="2022-01-01",
           date_to=None,
           commodities=None,
           ship_imo=None,
           unlocode=None,
           force_for_arrival_to_departure_greater_than=None):

    print("=== Arrival update ===")

    # We take dangling departures, and try to find the next arrival
    dangling_departures = departure.get_departures_without_arrival(min_dwt=min_dwt,
                                                                       commodities=commodities,
                                                                       date_from=date_from,
                                                                       date_to=date_to,
                                                                       ship_imo=ship_imo,
                                                                       unlocode=unlocode)
    if force_for_arrival_to_departure_greater_than is not None:
        dangling_departures.extend(departure.get_departures_with_arrival_too_remote_from_next_departure(
            min_timedelta=force_for_arrival_to_departure_greater_than,
            min_dwt=min_dwt,
            commodities=commodities,
            date_from=date_from,
            date_to=date_to,
            ship_imo=ship_imo,
            unlocode=unlocode))


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

    for d in tqdm(dangling_departures):
        departure_portcall = PortCall.query.filter(PortCall.id == d.portcall_id).first()
        imo = departure_portcall.ship_imo
        arrival_portcall = portcall.find_arrival(departure_portcall=departure_portcall)

        if arrival_portcall:

            existing_arrival = Arrival.query.filter(Arrival.departure_id == d.id).first()
            if existing_arrival is not None and existing_arrival.portcall_id != arrival_portcall.id:
                # Update
                existing_arrival.date_utc = arrival_portcall.date_utc
                existing_arrival.port_id = arrival_portcall.port_id
                existing_arrival.portcall_id = arrival_portcall.id
                existing_arrival.method_id = "python"
                session.commit()

                # And remove associated trajectories, berths etc
                existing_shipment = Shipment.query.filter(Shipment.departure_id == d.id).first()
                if existing_shipment is not None:
                    session.query(ShipmentArrivalBerth).filter(ShipmentArrivalBerth.shipment_id == existing_shipment.id).delete()
                    session.query(Trajectory).filter(Trajectory.shipment_id == existing_shipment.id).delete()

                session.commit()

            else:
                # There was no such arrival
                data = {
                    "departure_id": d.id,
                    "method_id": "python",
                    "date_utc": arrival_portcall.date_utc,
                    "port_id": arrival_portcall.port_id,
                    "portcall_id": arrival_portcall.id
                }
                arrival = Arrival(**data)
                session.add(arrival)
                try:
                    session.commit()
                except sqlalchemy.exc.IntegrityError:
                    logger.warning("Failed to push portcall. Probably missing port_id: %s" % (arrival.port_id,))
                    session.rollback()

        else:
            logger.info(
                "No relevant arrival found. Should check portcalls for imo %s from date %s." % (imo, d.date_utc))
