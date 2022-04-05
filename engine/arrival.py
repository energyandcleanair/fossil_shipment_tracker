import datetime as dt
import sqlalchemy
from tqdm import tqdm

import base
from engine import departure
from engine import portcall
from base.logger import logger
from base.db import session
from base.models import Arrival, Flow, Port, PortCall


def get_dangling_arrivals():
    subquery = session.query(Flow.arrival_id)
    return Arrival.query.filter(~Arrival.id.in_(subquery)).all()


def update(min_dwt=base.DWT_MIN,
           limit=None,
           date_from=None,
           commodities=[base.LNG,
                        base.CRUDE_OIL,
                        base.OIL_PRODUCTS,
                        base.OIL_OR_CHEMICAL,
                        base.COAL,
                        base.BULK]
           ):
    print("=== Arrival update ===")

    # We take dangling departures, and try to find the next arrival
    dangling_departures = departure.get_dangling_departures(min_dwt=min_dwt,
                                                            commodities=commodities,
                                                            date_from=date_from)

    if limit is not None:
        # For debugging without taking too many credits
        dangling_departures = dangling_departures[0:limit]

    # Very important to sort them by date, so that we don't miss any arrival
    # That would happen if a ship had two departure without yet an arrival
    # and we'd start looking from the latest departure
    dangling_departures.sort(key=lambda x: x.date_utc)

    for d in tqdm(dangling_departures):
        departure_portcall = PortCall.query.filter(PortCall.id == d.portcall_id).first()
        imo = departure_portcall.ship_imo
        arrival_portcall = portcall.find_arrival(departure_portcall=departure_portcall)
        if arrival_portcall:
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
