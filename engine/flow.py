import datetime as dt

import base
from base.db import session
from base.models import Ship, Departure, Flow, Position, Arrival
from engine.arrival import get_dangling_arrivals
from engine import position
from tqdm import tqdm
import sqlalchemy
from sqlalchemy import func


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


def update_positions(commodities=None):

    position_subq = session.query(
        Position.flow_id,
        func.max(Position.date_utc).label('last_date'),
        func.min(Position.date_utc).label('first_date')
    ).group_by(Position.flow_id).subquery('last_position')

    # We update position which are still ongoing (no arrival yet)
    # or who are still missing some positions (should have til Arrival + n hours, and from Departure - n_hours)
    flows_to_update = session.query(Flow, Departure.ship_imo, Departure.date_utc, Arrival.date_utc) \
        .join(Departure, Flow.departure_id == Departure.id) \
        .join(Ship, Ship.imo == Departure.ship_imo) \
        .join(Arrival, Flow.arrival_id == Arrival.id) \
        .join(position_subq, Flow.id == position_subq.c.flow_id) \
        .filter(sqlalchemy.or_(Flow.arrival_id == sqlalchemy.null(),
                               sqlalchemy.or_(
                                   position_subq.c.last_date <= Arrival.date_utc + dt.timedelta(hours=base.QUERY_POSITION_HOURS_AFTER_ARRIVAL),
                                   position_subq.c.first_date >= Departure.date_utc - dt.timedelta(hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE)
                                   )
                               )
                )

    if commodities is not None:
        flows_to_update = flows_to_update.filter(Ship.commodity.in_(commodities))

    flows_to_update = flows_to_update.all()
    # Add positions
    for f in tqdm(flows_to_update):
        flow = f[0]
        ship_imo = f[1]
        departure_date = f[2]
        arrival_date = f[3]

        date_from = departure_date - dt.timedelta(hours=base.QUERY_POSITION_HOURS_BEFORE_DEPARTURE)
        date_to = arrival_date + dt.timedelta(hours=base.QUERY_POSITION_HOURS_AFTER_ARRIVAL)

        first_date, last_date = session.query(
            func.min(Position.date_utc).label('first_date'),
            func.max(Position.date_utc).label('last_date')
            ).filter(Position.flow_id==flow.id).all()[0]

        dates = []

        if first_date is None:
            # No position found, we query the whole voyage
            dates.append({"date_from": date_from, "date_to": date_to})
        else:
            # We only query head or tail or both
            if first_date > date_from:
                dates.append({"date_from": date_from, "date_to": first_date})
            if last_date < date_to:
                dates.append({"date_from": last_date, "date_to": date_to})

        for date in dates:
            positions = position.get(imo=ship_imo, **date)
            if positions:
                for p in positions:
                    p.flow_id = flow.id
                    session.add(p)
            session.commit()
