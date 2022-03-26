import datetime as dt

from engine import departure
from engine import portcall
from base.db import session

from models import Arrival, Flow


def get_dangling_arrivals():
    subquery = session.query(Flow.arrival_id)
    return Arrival.query.filter(~Arrival.id.in_(subquery)).all()


def update(min_dwt=None, limit=None):

    # We take dangling departures, and try to find the next arrival
    dangling_departures = departure.get_dangling_departures(min_dwt=min_dwt)

    if limit is not None:
        # For debugging without taking too many credits
        dangling_departures = dangling_departures[0:limit]

    # Very important to sort them by date, so that we don't miss any arrival
    # That would happen if a ship had two departure without yet an arrival
    # and we'd start looking from the latest departure
    dangling_departures.sort(key=lambda x: x.date_utc)

    for d in dangling_departures:
        imo = d.ship_imo
        departure_date = d.date_utc

        # This is the filter that will be applied to arrival portcall to consider it legit
        # After manually inspecting some routes, we saw for instance that vessals would moore
        # away from departure terminal. This would have a unlocode=none
        # We also start 12 hours after departure

        filter = lambda x: x.port_unlocode is not None and x.port_unlocode != ""
        arrival_portcall = portcall.get_first_arrival_portcall(imo=imo,
                                                       date_from=departure_date + dt.timedelta(hours=12),
                                                       filter=filter)
        if arrival_portcall:
            data = {
                "departure_id": d.id,
                "method_id": "marinetraffic_portcall",
                "date_utc": arrival_portcall.date_utc,
                "port_unlocode": arrival_portcall.port_unlocode
            }
            arrival = Arrival(**data)
            session.add(arrival)
            session.commit()