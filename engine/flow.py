import datetime as dt

from base.db import session
from base.models import Ship, Departure, Flow, Position, Arrival
from engine.arrival import get_dangling_arrivals
from engine import position
from tqdm import tqdm
import sqlalchemy

def update():
    print("=== Flow update ===")

    # Collect arrivals that aren't translated into a flow yet
    dangling_arrivals = get_dangling_arrivals()

    for d in tqdm(dangling_arrivals):
        # imo = session.query(Departure.ship_imo).filter(Departure.id == d.departure_id)
        # ship = Ship.query.filter(Ship.imo.in_(imo)).first()
        # departure = Departure.query.filter(Departure.id == d.departure_id).first()

        data = {
            "departure_id": d.departure_id,
            "arrival_id": d.id
        }

        flow = Flow(**data)
        session.add(flow)
    session.commit()


def update_positions():
    subquery = session.query(Position.flow_id)
    flows_to_update = session.query(Flow, Departure.ship_imo, Departure.date_utc, Arrival.date_utc) \
        .join(Departure, Flow.departure_id == Departure.id) \
        .join(Arrival, Flow.arrival_id == Arrival.id) \
        .filter(sqlalchemy.or_(Flow.arrival_id==sqlalchemy.null(),
                               Flow.id.notin_(subquery))).all()

    # Add positions
    for f in tqdm(flows_to_update):
        flow = f[0]
        ship_imo = f[1]
        departure_date = f[2]
        arrival_date = f[3]
        date_from = session.query(Position.date_utc).filter(Position.flow_id==flow.id) \
            .order_by(Position.date_utc.desc()) \
            .first()

        if date_from is None:
            date_from = departure_date
        else:
            date_from = date_from[0]

        date_to = arrival_date
        positions = position.get(imo=ship_imo, date_from=date_from, date_to=date_to)
        if positions:
            for p in positions:
                p.flow_id = flow.id
                session.add(p)
        session.commit()

        # positions = position.get(imo=imo, date_from=departure.date_utc, date_to=d.date_utc)
        # if positions:
        #
        # session.commit()


