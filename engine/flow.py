from base.db import session
from models import Arrival, Ship, Departure, Flow
from engine.arrival import get_dangling_arrivals


def update():

    # We take dangling departures, and try to find the next arrival
    dangling_arrivals = get_dangling_arrivals()

    for d in dangling_arrivals:
        imo = session.query(Departure.ship_imo).filter(Departure.id == d.departure_id)
        ship = Ship.query.filter(Ship.imo.in_(imo)).first()

        data = {
            "departure_id": d.departure_id,
            "arrival_id": d.id
        }

        flow = Flow(**data)
        session.add(flow)

    session.commit()


